import pandas as pd
import logging
import traceback
from app.config.db_config import get_db_connection, DB_ALLINONE, DB_TARGET

# **Fungsi untuk Membuat Tabel payment_cleaned Jika Belum Ada**
def create_table_if_not_exists():
    """Membuat tabel payments_cleaned jika belum ada di database."""
    logging.info("🔹 Mengecek apakah tabel `payments_cleaned` sudah ada di MySQL...")

    conn_target = get_db_connection(DB_TARGET)
    if not conn_target:
        logging.error("❌ Gagal terhubung ke database target!")
        return False  # Pastikan return False jika gagal

    cursor_target = conn_target.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS payments_cleaned (
        id INT(10) UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
        id_auto_payment BIGINT(20) DEFAULT NULL,
        id_manual_payment BIGINT(20) DEFAULT NULL,
        id_idn_payment BIGINT(20) DEFAULT NULL,
        id_briva_payment BIGINT(20) DEFAULT NULL,
        date DATE NOT NULL,
        regcode VARCHAR(13) NOT NULL,
        type VARCHAR(15) NOT NULL,
        channel VARCHAR(191) DEFAULT NULL,
        method VARCHAR(50) NOT NULL,
        bank VARCHAR(50) DEFAULT NULL,
        nominal DECIMAL(15,4) NOT NULL,
        edcCost DECIMAL(15,4) NOT NULL DEFAULT 0.0000,
        description TEXT NOT NULL,
        created_at TIMESTAMP NULL DEFAULT NULL,
        updated_at TIMESTAMP NULL DEFAULT NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_unicode_ci;
    """

    try:
        cursor_target.execute(create_table_query)
        conn_target.commit()
        logging.info("✅ Tabel `payments_cleaned` sudah tersedia atau berhasil dibuat!")
        return True  # Pastikan return True jika sukses
    except Exception as e:
        logging.error(f"❌ Gagal membuat tabel `payments_cleaned`: {str(e)}")
        return False
    finally:
        cursor_target.close()
        conn_target.close()


# **Proses ETL**
def etl_process():
    try:
        logging.info("🔹 Mengecek dan membuat tabel jika belum ada...")
        table_created = create_table_if_not_exists()  # Simpan hasil return

        if not table_created:
            return {"error": "Gagal membuat tabel `payment_cleaned`"}, 500 

        logging.info("🔹 Mengambil data dari database sumber...")
        conn_source = get_db_connection(DB_ALLINONE)
        if not conn_source:
            return {"error": "Koneksi ke database sumber gagal!"}, 500

        df = pd.read_sql("SELECT * FROM payment WHERE id IS NOT NULL", con=conn_source)
        conn_source.close()

        if df.empty:
            logging.warning("⚠️ Data dari database kosong!")
            return {"message": "Tidak ada data untuk diproses", "new_records": 0}

        logging.info(f"✅ Data berhasil diambil: {len(df)} records")

        # ✅ Membersihkan data
        df.drop(columns=["updated_by", "created_at", "updated_at"], errors="ignore", inplace=True)
        df.dropna(subset=["id"], inplace=True)
        df["id"] = pd.to_numeric(df["id"], errors="coerce").fillna(0).astype(int)

        df.fillna({
            "id_auto_payment": 0, "id_manual_payment": 0, "id_idn_payment": 0, 
            "id_briva_payment": 0, "nominal": 0, "edcCost": 0, "description": "UNKNOWN",
            "bank": "UNKNOWN", "method": "UNKNOWN", "channel": "UNKNOWN"
        }, inplace=True)

        # ✅ Konversi Tipe Data
        num_cols = ["id_auto_payment", "id_manual_payment", "id_idn_payment", 
                    "id_briva_payment", "nominal", "edcCost"]
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce").where(df["date"].notna(), None)

        df.drop_duplicates(subset=["id"], inplace=True)
        logging.info(f"✅ Data dibersihkan: {len(df)} records")

        # ✅ Koneksi ke database tujuan
        conn_target = get_db_connection(DB_TARGET)
        if not conn_target:
            return {"error": "Koneksi ke database target gagal!"}, 500

        cursor_target = conn_target.cursor()

        # ✅ Ambil ID yang sudah ada di tabel tujuan
        cursor_target.execute("SELECT id FROM payments_cleaned")
        existing_ids = {str(row[0]) for row in cursor_target.fetchall()}
        logging.info(f"🔹 Data yang sudah ada di payments_cleaned: {len(existing_ids)}")

        # ✅ Filter hanya data baru
        df["id"] = df["id"].astype(str)
        df_new = df[~df["id"].isin(existing_ids)]
        logging.info(f"🔍 Data baru yang akan dimasukkan: {len(df_new)} records")

        if df_new.empty:
            logging.info("✅ Tidak ada data baru untuk dimasukkan.")
            cursor_target.close()
            conn_target.close()
            return {"message": "ETL selesai", "new_records": 0}

        df_new = df_new.replace({pd.NA: None})

        required_cols = ["id", "id_auto_payment", "id_manual_payment", "id_idn_payment", 
                         "id_briva_payment", "date", "regcode", "type", "channel", 
                         "method", "bank", "nominal", "edcCost", "description"]
        for col in required_cols:
            if col not in df_new.columns:
                df_new[col] = None

        df_new = df_new[required_cols]

        # ✅ Insert ke database
        sql_insert = """
            INSERT IGNORE INTO payments_cleaned 
            (id, id_auto_payment, id_manual_payment, id_idn_payment, id_briva_payment, date, regcode, type, 
            channel, method, bank, nominal, edcCost, description) 
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        cleaned_tuples = [tuple(row) for row in df_new.itertuples(index=False, name=None)]
        cursor_target.executemany(sql_insert, cleaned_tuples)
        conn_target.commit()
        logging.info(f"✅ {len(cleaned_tuples)} records berhasil dimasukkan!")

        cursor_target.close()
        conn_target.close()
        logging.info("✅ Proses ETL selesai!")

        return {"message": "ETL Process Completed", "new_records": len(cleaned_tuples)}

    except Exception as e:
        logging.error(f"❌ Error ETL: {str(e)}")
        logging.error(f"🔍 Stacktrace:\n{traceback.format_exc()}")
        return {"error": str(e), "message": "Gagal menjalankan ETL"}, 500
