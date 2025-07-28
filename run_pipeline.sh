#!/bin/bash

# 🚀 Retail Sales Pipeline - Tam Otomatik Çalıştırma
# Bu script tüm pipeline'ı sırasıyla çalıştırır

echo "🚀 Retail Sales Pipeline Başlatılıyor..."
echo "================================================"

# Proje dizinine git
cd "$(dirname "$0")"
export AIRFLOW_HOME=$(pwd)/airflow

echo "📁 Proje dizini: $(pwd)"
echo ""

# 1. Bronze Ingestion
echo "🥉 1️⃣ BRONZE LAYER - Data Ingestion"
echo "CSV'den Delta formatına veri yükleniyor..."
python3 tasks/ingest_data.py
if [ $? -eq 0 ]; then
    echo "✅ Bronze ingestion başarılı!"
else
    echo "❌ Bronze ingestion başarısız!"
    exit 1
fi
echo ""

# 2. Silver Cleaning
echo "🥈 2️⃣ SILVER LAYER - Data Cleaning"
echo "Veri temizleme ve standardizasyon..."
python3 tasks/clean_data.py
if [ $? -eq 0 ]; then
    echo "✅ Silver cleaning başarılı!"
else
    echo "❌ Silver cleaning başarısız!"
    exit 1
fi
echo ""

# 3. Gold Aggregation
echo "🥇 3️⃣ GOLD LAYER - Business Analytics"
echo "İş analitiği tabloları oluşturuluyor..."
python3 tasks/generate_gold.py
if [ $? -eq 0 ]; then
    echo "✅ Gold aggregation başarılı!"
else
    echo "❌ Gold aggregation başarısız!"
    exit 1
fi
echo ""

# 4. Advanced Analytics
echo "🔬 4️⃣ ADVANCED ANALYTICS - RFM, CLTV, Forecasting"
echo "İleri düzey müşteri analitiği..."
python3 tasks/advanced_analytics.py
if [ $? -eq 0 ]; then
    echo "✅ Advanced analytics başarılı!"
else
    echo "❌ Advanced analytics başarısız!"
    exit 1
fi
echo ""

# Özet
echo "🎉 Pipeline Başarıyla Tamamlandı!"
echo "================================================"
echo "📊 Oluşturulan tablolar:"
echo "   🥉 Bronze: delta/bronze/online_retail"
echo "   🥈 Silver: delta/silver/online_retail_cleaned"  
echo "   🥇 Gold: delta/gold/daily_sales, top_products, country_sales"
echo "   🔬 Analytics: delta/gold/rfm_table, cltv_table, forecast_table"
echo ""
echo "🚀 Sonraki adımlar:"
echo "   📊 Dashboard: streamlit run dashboard/app.py"
echo "   🌐 Airflow UI: http://localhost:8080 (webserver başlatılmışsa)"
echo "   📈 Verileri analiz etmek için Gold tablolarını kullanabilirsiniz!"
echo ""
echo "💡 Airflow ile otomatik çalıştırma:"
echo "   airflow dags trigger retail_sales_pipeline"
