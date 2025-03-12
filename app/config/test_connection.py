import sys
import os
import pymysql

# Menambahkan path agar bisa import module `app`
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))

from app.config.db_config import get_db_connection, DB_ALL_IN_ONE, DB_MAIN

def test_connection():
    """Menghubungkan ke database dan menampilkan 2 tabel pertama sebagai bukti koneksi."""
    try:
        print("\n🔹 Menguji koneksi ke database ALL_IN_ONE...")
        conn = get_db_connection(DB_ALL_IN_ONE)
        if conn:
            print("✅ Koneksi ke database ALL_IN_ONE berhasil!")
            cursor = conn.cursor()

            # ✅ Ambil daftar tabel dari database
            cursor.execute("SHOW TABLES")
            rows = cursor.fetchall()

            # ✅ Periksa format data apakah tuple atau dictionary
            if rows and isinstance(rows[0], tuple):
                tables = [row[0] for row in rows]  # ✅ Gunakan tuple indexing
            else:
                tables = [row["Tables_in_" + DB_ALL_IN_ONE["database"]] for row in rows]  # ✅ Jika dictionary

            print(f"📋 Daftar tabel: {tables[:2]}")  # Tampilkan 2 tabel pertama

            # ✅ Ambil beberapa data dari tabel pertama jika ada
            if tables:
                table_name = tables[0]
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                records = cursor.fetchall()
                print(f"📌 Contoh data dari `{table_name}`: {records}")

            conn.close()
        
        print("\n🔹 Menguji koneksi ke database MAIN...")
        conn = get_db_connection(DB_MAIN)
        if conn:
            print("✅ Koneksi ke database MAIN berhasil!")
            cursor = conn.cursor()

            # ✅ Ambil daftar tabel dari database
            cursor.execute("SHOW TABLES")
            rows = cursor.fetchall()

            # ✅ Periksa format data apakah tuple atau dictionary
            if rows and isinstance(rows[0], tuple):
                tables = [row[0] for row in rows]  # ✅ Gunakan tuple indexing
            else:
                tables = [row["Tables_in_" + DB_MAIN["database"]] for row in rows]  # ✅ Jika dictionary

            print(f"📋 Daftar tabel: {tables[:2]}")  # Tampilkan 2 tabel pertama

            # ✅ Ambil beberapa data dari tabel pertama jika ada
            if tables:
                table_name = tables[0]
                cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
                records = cursor.fetchall()
                print(f"📌 Contoh data dari `{table_name}`: {records}")

            conn.close()

    except pymysql.MySQLError as e:
        print(f"❌ Error koneksi ke database: {str(e)}")

if __name__ == "__main__":
    test_connection()
