#!/bin/bash

# 🚀 Retail Sales Pipeline - Complete Automation Script
# Bu script tüm pipeline'ı otomatik olarak çalıştırır

echo "🛍️ ===== RETAIL SALES PIPELINE STARTING ===== 🛍️"
echo "📅 $(date)"
echo ""

# Proje dizinine git
cd /Users/alialtunoglu/Desktop/realtime-sales-pipeline

# Conda environment aktif et
echo "🔧 Activating conda environment..."
source /opt/miniconda3/bin/activate spark-delta-env

# 1️⃣ BRONZE LAYER - Data Ingestion
echo ""
echo "🥉 ===== BRONZE LAYER: DATA INGESTION ====="
python3 tasks/ingest_data.py
if [ $? -eq 0 ]; then
    echo "✅ Bronze ingestion completed successfully"
else
    echo "❌ Bronze ingestion failed"
    exit 1
fi

# 2️⃣ SILVER LAYER - Data Cleaning
echo ""
echo "🥈 ===== SILVER LAYER: DATA CLEANING ====="
python3 tasks/clean_data.py
if [ $? -eq 0 ]; then
    echo "✅ Silver cleaning completed successfully"
else
    echo "❌ Silver cleaning failed"
    exit 1
fi

# 3️⃣ GOLD LAYER - Business Analytics
echo ""
echo "🥇 ===== GOLD LAYER: BUSINESS ANALYTICS ====="
python3 tasks/generate_gold.py
if [ $? -eq 0 ]; then
    echo "✅ Gold aggregation completed successfully"
else
    echo "❌ Gold aggregation failed"
    exit 1
fi

# 4️⃣ ADVANCED ANALYTICS - RFM, CLTV, Forecasting
echo ""
echo "🔬 ===== ADVANCED ANALYTICS ====="
python3 tasks/advanced_analytics.py
if [ $? -eq 0 ]; then
    echo "✅ Advanced analytics completed successfully"
else
    echo "❌ Advanced analytics failed"
    exit 1
fi

# 5️⃣ SUMMARY & DASHBOARD
echo ""
echo "📊 ===== PIPELINE SUMMARY ====="
echo "🥉 Bronze: Raw data ingested"
echo "🥈 Silver: Data cleaned and validated"
echo "🥇 Gold: Business analytics tables created"
echo "🔬 Advanced: RFM, CLTV, Forecasting completed"
echo ""
echo "📈 DASHBOARD READY!"
echo "To start dashboard, run:"
echo "   conda activate spark-delta-env"
echo "   streamlit run dashboard/app.py --server.port 8504"
echo ""
echo "🎉 ===== PIPELINE COMPLETED SUCCESSFULLY ===== 🎉"
echo "📅 $(date)"
