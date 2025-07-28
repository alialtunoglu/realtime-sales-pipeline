"""
🥈 Silver Layer DAG - Sadece Data Cleaning
Bronze'dan Silver'a veri temizleme
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
import os

# Proje kök dizinini ekle
project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
sys.path.append(os.path.join(project_root, 'tasks'))

from clean_data import run_silver_cleaning

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 28),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'silver_cleaning_dag',
    default_args=default_args,
    description='🥈 Silver Layer - Data Cleaning & Validation',
    schedule_interval=None,  # Manuel tetikleme
    catchup=False,
    tags=['silver', 'cleaning', 'delta'],
)

silver_cleaning_task = PythonOperator(
    task_id='run_cleaning',
    python_callable=run_silver_cleaning,
    dag=dag,
)

dag.doc_md = """
# 🥈 Silver Layer Cleaning

Bu DAG sadece **Silver katmanı** veri temizleme işlemini yapar.

**İşlem:**
- `delta/bronze/online_retail` → `delta/silver/online_retail_cleaned`
- Null değerleri temizler
- Veri tiplerini düzeltir
- Geçersiz kayıtları filtreler
"""
