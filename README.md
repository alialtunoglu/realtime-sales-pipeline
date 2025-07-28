# 🛍️ Real-time Sales Pipeline

Modern **Medallion Architecture** ile geliştirilmiş retail satış veri pipeline'ı. Apache Spark, Delta Lake, ve Apache Airflow kullanarak Bronze-Silver-Gold katmanlı veri işleme sistemi.

## 🏗️ Mimari

```
📁 CSV Data → 🥉 Bronze → 🥈 Silver → 🥇 Gold → 🔬 Advanced Analytics → 📊 Dashboard
```

### Katmanlar:
- **🥉 Bronze:** Ham veri (Delta format)
- **🥈 Silver:** Temizlenmiş veri (Validated)  
- **🥇 Gold:** İş analitiği tabloları
- **🔬 Advanced:** RFM, CLTV, Forecasting

## 🚀 Hızlı Başlangıç

### 1️⃣ Ortam Hazırlığı
```bash
# Conda environment aktif et
conda activate spark-delta-env

# Gerekli paketleri kur (eğer yoksa)
pip install pyspark delta-spark apache-airflow streamlit plotly
```

### 2️⃣ Pipeline'ı Çalıştır
```bash
# Hızlı test
./test_pipeline.sh

# Tam pipeline
./run_complete_pipeline.sh
```

### 3️⃣ Dashboard'u Başlat
```bash
streamlit run dashboard/app.py --server.port 8504
```

## 🤖 Airflow Otomasyonu

### DAG'ları Çalıştır
```bash
# Airflow başlat
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080 &
airflow scheduler &

# Pipeline tetikle
airflow dags trigger retail_sales_pipeline
```

### Kullanılabilir DAG'lar:
- `retail_sales_pipeline` - Tam pipeline
- `bronze_ingestion_dag` - Sadece Bronze layer
- `silver_cleaning_dag` - Sadece Silver layer

## 📊 Oluşturulan Tablolar

### Gold Layer:
- `daily_sales` - Günlük satış özetleri
- `top_products` - En çok satılan ürünler  
- `country_sales` - Ülke bazlı satış

### Advanced Analytics:
- `rfm_table` - Müşteri segmentasyonu
- `cltv_table` - Müşteri yaşam boyu değeri
- `forecast_table` - 12 aylık gelir tahminleri

## 📁 Proje Yapısı

```
├── airflow/
│   ├── dags/                    # Airflow DAG dosyaları
│   └── airflow.cfg             # Airflow konfigürasyonu
├── dashboard/
│   └── app.py                  # Streamlit dashboard
├── data/
│   ├── input/                  # Ham veri dosyaları
│   └── output/                 # İşlenmiş veriler
├── delta/                      # Delta Lake tabloları
│   ├── bronze/                 # Ham veriler
│   ├── silver/                 # Temizlenmiş veriler
│   └── gold/                   # İş analitiği
├── notebooks/                  # Jupyter Notebooks
│   ├── Complete_Pipeline_Tutorial.ipynb
│   ├── 01_ingestion.ipynb
│   ├── 02_silver_cleaning.ipynb
│   ├── 03_gold_aggregation.ipynb
│   ├── gold_rfm.ipynb
│   ├── gold_cltv.ipynb
│   └── gold_forecasting.ipynb
├── tasks/                      # Pipeline task'leri
│   ├── ingest_data.py          # Bronze layer
│   ├── clean_data.py           # Silver layer
│   ├── generate_gold.py        # Gold layer
│   └── advanced_analytics.py   # RFM, CLTV, Forecasting
├── run_complete_pipeline.sh    # Tam pipeline script
├── test_pipeline.sh           # Hızlı test script
└── requirements.txt           # Python dependencies
```

## 🛠️ Teknoloji Stack

- **Veri İşleme:** Apache Spark + Delta Lake
- **Orchestration:** Apache Airflow
- **Visualization:** Streamlit + Plotly
- **Analytics:** PySpark SQL + MLlib
- **Storage:** Delta Lake (Parquet + Transaction Log)

## 📈 Analytics Özellikleri

### RFM Analizi
- **Recency:** Son alışverişten bu yana geçen gün
- **Frequency:** Toplam alışveriş sayısı  
- **Monetary:** Toplam harcama miktarı

### CLTV (Customer Lifetime Value)
```
CLTV = (Frequency × Monetary) / (Recency + 1)
```

### Forecasting
```
12 Aylık Tahmini Gelir = CLTV × 12
```

## 🔧 Konfigürasyon

### Spark Ayarları
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

### Delta Lake Ayarları
```python
spark.conf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
spark.conf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
```

## 📚 Öğrenme Kaynakları

1. **📓 Complete_Pipeline_Tutorial.ipynb** - Tam tutorial
2. **📊 Dashboard** - http://localhost:8504
3. **🤖 Airflow UI** - http://localhost:8080

## 🐛 Sorun Giderme

### Streamlit TypeIs Hatası
```bash
pip install --upgrade typing-extensions
```

### Spark Port Çakışması
```bash
# Farklı port kullan
spark.conf.set("spark.ui.port", "4041")
```

### Delta Table Okuma Hatası
```bash
# Cache temizle
spark.sql("CLEAR CACHE")
```

## 🤝 Katkıda Bulunma

1. Fork yapın
2. Feature branch oluşturun (`git checkout -b feature/amazing-feature`)
3. Commit yapın (`git commit -m 'Add amazing feature'`)
4. Push yapın (`git push origin feature/amazing-feature`)
5. Pull Request açın

## 📄 Lisans

Bu proje MIT lisansı altında lisanslanmıştır.

## 👨‍💻 Geliştirici

Medallion Architecture ve modern veri mühendisliği prensipleri ile geliştirilmiştir.

---

**🎯 Bu proje ile neler öğreneceksiniz:**
- Modern veri mühendisliği best practice'leri
- Apache Spark ile büyük veri işleme
- Delta Lake ile ACID transaction'lar
- Apache Airflow ile pipeline otomasyonu
- Streamlit ile real-time dashboard'lar
- RFM, CLTV ve forecasting analitiği
