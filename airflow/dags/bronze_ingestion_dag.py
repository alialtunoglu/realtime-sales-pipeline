"""
🥉 Bronze Layer DAG - Sadece Data Ingestion
CSV'den Delta formatına veri yükleme
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Proje kök dizinini ekle
project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
sys.path.append(os.path.join(project_root, 'tasks'))

from ingest_data import run_bronze_ingestion

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 28),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'bronze_ingestion_dag',
    default_args=default_args,
    description='🥉 Bronze Layer - CSV to Delta Ingestion',
    schedule_interval=None,  # Manuel tetikleme
    catchup=False,
    tags=['bronze', 'ingestion', 'delta'],
)

bronze_ingestion_task = PythonOperator(
    task_id='run_bronze_ingestion',
    python_callable=run_bronze_ingestion,
    dag=dag,
)

dag.doc_md = """
# 🥉 Bronze Layer Ingestion

Bu DAG sadece **Bronze katmanı** veri yükleme işlemini yapar.

**İşlem:**
- `data/input/retail_data.csv` → `delta/bronze/online_retail`
- Ham veri, hiç değişiklik yapılmaz
"""
