"""
🚀 Retail Sales Pipeline - Complete Data Pipeline
Medallion Architecture ile tam otomatik veri pipeline'ı

Pipeline Akışı:
Bronze (Raw) → Silver (Clean) → Gold (Analytics) → Advanced Analytics

Author: AI Assistant
Date: 2025-07-28
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Proje kök dizinini Airflow'a ekle
project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
sys.path.append(os.path.join(project_root, 'tasks'))

# Task fonksiyonlarını import et
from ingest_data import run_bronze_ingestion
from clean_data import run_silver_cleaning  
from generate_gold import run_gold_aggregation
from advanced_analytics import run_advanced_analytics

# 🔧 DAG Konfigürasyonu
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# 📋 DAG tanımı
dag = DAG(
    'retail_sales_pipeline',
    default_args=default_args,
    description='🛍️ Complete Retail Sales Data Pipeline with Medallion Architecture',
    schedule_interval='@daily',  # Günlük çalışır
    catchup=False,
    tags=['retail', 'delta', 'spark', 'medallion', 'analytics'],
)

# 🥉 BRONZE LAYER - Raw Data Ingestion
bronze_task = PythonOperator(
    task_id='bronze_ingestion',
    python_callable=run_bronze_ingestion,
    dag=dag,
    doc_md="""
    ## Bronze Layer - Data Ingestion
    
    **What it does:**
    - Reads CSV file from `data/input/retail_data.csv`
    - Saves raw data to Delta format in `delta/bronze/online_retail`
    - No transformations, just raw data storage
    
    **Output:** Raw Delta table in Bronze layer
    """
)

# 🥈 SILVER LAYER - Data Cleaning
silver_task = PythonOperator(
    task_id='silver_cleaning',
    python_callable=run_silver_cleaning,
    dag=dag,
    doc_md="""
    ## Silver Layer - Data Cleaning
    
    **What it does:**
    - Reads from Bronze Delta table
    - Removes null values and invalid records
    - Standardizes data types and formats
    - Filters positive quantities and prices
    - Saves clean data to `delta/silver/online_retail_cleaned`
    
    **Output:** Clean Delta table in Silver layer
    """
)

# 🥇 GOLD LAYER - Business Analytics
gold_task = PythonOperator(
    task_id='gold_aggregation',
    python_callable=run_gold_aggregation,
    dag=dag,
    doc_md="""
    ## Gold Layer - Business Analytics
    
    **What it does:**
    - Creates business-ready analytical tables:
      - Daily sales summaries
      - Top selling products
      - Country-wise revenue
    - Saves to multiple Gold Delta tables
    
    **Output:** Multiple analytical Delta tables
    """
)

# 🔬 ADVANCED ANALYTICS - RFM, CLTV, Forecasting
advanced_task = PythonOperator(
    task_id='advanced_analytics',
    python_callable=run_advanced_analytics,
    dag=dag,
    doc_md="""
    ## Advanced Analytics - Customer Intelligence
    
    **What it does:**
    - **RFM Analysis:** Customer segmentation (Recency, Frequency, Monetary)
    - **CLTV Analysis:** Customer Lifetime Value calculation
    - **Forecasting:** 12-month revenue predictions
    
    **Output:** Customer intelligence Delta tables
    """
)

# 📊 DASHBOARD REFRESH (Optional)
dashboard_refresh = BashOperator(
    task_id='refresh_dashboard',
    bash_command=f"""
    echo "🔄 Dashboard verilerini yenileniyor..."
    echo "📊 Gold layer tabloları hazır!"
    echo "📈 Streamlit dashboard'u: python {project_root}/dashboard/app.py"
    echo "✅ Pipeline tamamlandı!"
    """,
    dag=dag,
    doc_md="""
    ## Dashboard Refresh
    
    **What it does:**
    - Notifies that all data is ready
    - Dashboard can now display fresh data
    - Provides instructions to start Streamlit
    """
)

# 🔗 TASK DEPENDENCİLERİ (Pipeline Sırası)
bronze_task >> silver_task >> gold_task >> advanced_task >> dashboard_refresh

# 📝 DAG Açıklaması
dag.doc_md = """
# 🛍️ Retail Sales Data Pipeline

Bu pipeline, online retail satış verilerini **Medallion Architecture** kullanarak işler.

## 🏗️ Pipeline Mimarisi

```
📁 CSV Data
    ↓
🥉 Bronze (Raw Delta)
    ↓
🥈 Silver (Clean Delta)
    ↓
🥇 Gold (Analytics Delta)
    ↓
🔬 Advanced Analytics
    ↓
📊 Dashboard Ready!
```

## 📊 Oluşturulan Tablolar

### Gold Layer:
- `daily_sales` - Günlük satış özetleri
- `top_products` - En çok satılan ürünler  
- `country_sales` - Ülke bazlı gelir

### Advanced Analytics:
- `rfm_table` - Müşteri segmentasyonu
- `cltv_table` - Müşteri yaşam boyu değeri
- `forecast_table` - 12 aylık gelir tahminleri

## 🚀 Çalıştırma

Pipeline günlük otomatik çalışır veya manuel tetiklenebilir.
"""
