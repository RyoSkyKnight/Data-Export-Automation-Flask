import os
import sys
import logging
from flask import Flask, jsonify

# Menambahkan path agar bisa import module `app`
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from app.services.etl_payments_data import etl_process

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

app = Flask(__name__)

@app.route("/")
def home():
    return jsonify({"message": "Welcome to the ETL API!"})

@app.route("/export-data/payments", methods=['GET'])
def export_payments_data():
    """Memanggil proses ETL dari API."""
    try:
        logging.info("🔹 Memulai proses ETL melalui API...")
        result = etl_process()

        # Pastikan response dalam format JSON
        if isinstance(result, tuple):  
            return jsonify(result[0]), result[1]

        logging.info("✅ Proses ETL berhasil dijalankan!")
        return jsonify({"status": "success", "message": "ETL process completed", "data": result})

    except Exception as e:
        logging.error(f"❌ Terjadi kesalahan saat menjalankan ETL: {str(e)}")
        return jsonify({"status": "error", "message": "Gagal menjalankan ETL", "error": str(e)}), 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 3000))
    logging.info(f"🚀 Menjalankan Flask di http://0.0.0.0:{port}")
    app.run(debug=True, host="0.0.0.0", port=port)
