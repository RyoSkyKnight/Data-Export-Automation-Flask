import sys
from airflow import DAG
# Menambahkan path agar bisa import module `app`
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../")))
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging
from app.services.etl_payments_data import etl_process, count_records

# Setup DAG
dag = DAG(
    'etl_payments',
    default_args={'owner': 'airflow', 'start_date': datetime(2025, 1, 29)},
    schedule_interval='@daily',
    catchup=False
)

# Task ETL
task_etl = PythonOperator(
    task_id='etl_process',
    python_callable=etl_process,
    dag=dag
)

# Task Hitung Data
task_count = PythonOperator(
    task_id='count_records',
    python_callable=count_records,
    dag=dag
)

# Menjalankan task_count setelah ETL selesai
task_etl >> task_count
