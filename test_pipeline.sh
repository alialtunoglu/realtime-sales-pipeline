#!/bin/bash

# 🧪 Quick Pipeline Test Script
# Pipeline'ı hızlıca test etmek için

echo "🧪 ===== QUICK PIPELINE TEST ====="
echo ""

cd /Users/alialtunoglu/Desktop/realtime-sales-pipeline

# Environment aktif et
echo "🔧 Activating environment..."
source /opt/miniconda3/bin/activate spark-delta-env

echo ""
echo "🎯 Testing Bronze → Silver → Gold → Advanced Analytics"
echo ""

# Bronze test
echo "🥉 Testing Bronze layer..."
python3 -c "
from tasks.ingest_data import run_bronze_ingestion
try:
    run_bronze_ingestion()
    print('✅ Bronze test passed')
except Exception as e:
    print(f'❌ Bronze test failed: {e}')
"

echo ""

# Silver test  
echo "🥈 Testing Silver layer..."
python3 -c "
from tasks.clean_data import run_silver_cleaning
try:
    run_silver_cleaning()
    print('✅ Silver test passed')
except Exception as e:
    print(f'❌ Silver test failed: {e}')
"

echo ""

# Gold test
echo "🥇 Testing Gold layer..."
python3 -c "
from tasks.generate_gold import run_gold_aggregation
try:
    run_gold_aggregation()
    print('✅ Gold test passed')
except Exception as e:
    print(f'❌ Gold test failed: {e}')
"

echo ""

# Advanced test
echo "🔬 Testing Advanced Analytics..."
python3 -c "
from tasks.advanced_analytics import run_advanced_analytics
try:
    run_advanced_analytics()
    print('✅ Advanced Analytics test passed')
except Exception as e:
    print(f'❌ Advanced Analytics test failed: {e}')
"

echo ""
echo "🎉 ===== PIPELINE TEST COMPLETED ====="
echo ""
echo "📊 Generated Tables:"
echo "   🥉 delta/bronze/online_retail"
echo "   🥈 delta/silver/online_retail_cleaned" 
echo "   🥇 delta/gold/daily_sales"
echo "   🥇 delta/gold/top_products"
echo "   🥇 delta/gold/country_sales"
echo "   🔬 delta/gold/rfm_table"
echo "   🔬 delta/gold/cltv_table"
echo "   🔬 delta/gold/forecast_table"
echo ""
echo "📈 To start dashboard:"
echo "   streamlit run dashboard/app.py --server.port 8504"
echo ""
echo "🤖 To use Airflow:"
echo "   export AIRFLOW_HOME=\$(pwd)/airflow"
echo "   airflow dags trigger retail_sales_pipeline"
