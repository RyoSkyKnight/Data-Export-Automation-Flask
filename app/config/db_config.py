import os
import json
import pymysql
from dotenv import load_dotenv

# ✅ Load environment variables dari .env
load_dotenv()

# ✅ Parse JSON dari .env
DB_ALLINONE = json.loads(os.getenv("DB_ALLINONE", "{}"))
DB_TARGET = json.loads(os.getenv("DB_TARGET", "{}"))

def get_db_connection(db_config):
    """Membuka koneksi ke database menggunakan pymysql."""
    try:
        conn = pymysql.connect(
            host=db_config["host"],
            port=db_config["port"],
            user=db_config["user"],
            password=db_config["password"],
            database=db_config["database"]
        )
        print(f"✅ Koneksi ke {db_config['database']} berhasil!")
        return conn
    except pymysql.MySQLError as e:
        print(f"❌ Gagal terhubung ke {db_config['database']}: {e}")
        return None
