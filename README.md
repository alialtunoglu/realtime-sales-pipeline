# 🛍️ Real-time Sales Pipeline

Modern **Medallion Architecture** ile geliştirilmiş enterprise-grade retail satış veri pipeline'ı. Apache Spark, Delta Lake, Apache Airflow, ve kapsamlı monitoring sistemi ile production-ready veri işleme platformu.

[![CI/CD](https://github.com/alialtunoglu/realtime-sales-pipeline/workflows/Pipeline%20CI/CD/badge.svg)](https://github.com/alialtunoglu/realtime-sales-pipeline/actions)
[![Coverage](https://codecov.io/gh/alialtunoglu/realtime-sales-pipeline/branch/main/graph/badge.svg)](https://codecov.io/gh/alialtunoglu/realtime-sales-pipeline)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=realtime-sales-pipeline&metric=alert_status)](https://sonarcloud.io/dashboard?id=realtime-sales-pipeline)

## 🏗️ Sistem Mimarisi

```
📁 Data Sources → 🥉 Bronze → 🥈 Silver → 🥇 Gold → 🤖 ML Models → 📊 Dashboard
                    ↓         ↓         ↓          ↓
                 🔍 Monitoring & Alerting System
```

### Katmanlar:
- **🥉 Bronze:** Ham veri (Delta format, schema validation)
- **🥈 Silver:** Temizlenmiş veri (Quality checks, business rules)  
- **🥇 Gold:** İş analitiği tabloları (Aggregated metrics)
- **🤖 ML Layer:** Forecasting & segmentation models
- **� Monitoring:** Real-time health checks & alerting

## ✨ Özellikler

### 📊 Veri İşleme
- **Medallion Architecture** ile katmanlı veri işleme
- **Delta Lake** ile ACID transactions ve time travel
- **Apache Spark** ile ölçeklenebilir veri işleme
- **Apache Airflow** ile orkestrasyon

### 🤖 Makine Öğrenmesi
- Satış tahminleme (Sales Forecasting)
- Müşteri segmentasyonu (RFM Analysis)
- Customer Lifetime Value (CLTV) hesaplama
- Model performans izleme

### 🔍 Monitoring & Observability
- Real-time sistem sağlık kontrolü
- Veri kalitesi metrikleri
- Model performans izleme
- Otomatik alert sistemi
- Interactive dashboard (Streamlit)

### 🚀 Production Ready
- Docker containerization
- Kubernetes deployment manifests
- CI/CD pipeline (GitHub Actions)
- Comprehensive testing (55 unit tests)
- Security scanning
- Performance monitoring

## 🚀 Hızlı Başlangıç

### 1️⃣ Development Environment

```bash
# Repository'yi klonla
git clone https://github.com/alialtunoglu/realtime-sales-pipeline.git
cd realtime-sales-pipeline

# Virtual environment oluştur
python -m venv retail-analytics
source retail-analytics/bin/activate  # Linux/Mac
# retail-analytics\Scripts\activate  # Windows

# Dependencies kur
pip install -r requirements.txt
```

### 2️⃣ Quick Start
```bash
# Pipeline'ı test et
python test_pipeline.sh

# Dashboard'u başlat
streamlit run dashboard/app.py

# Monitoring başlat
python monitoring.py run

# CLI aracını kullan
python -m src.monitoring.cli status
```

### 3️⃣ Production Deployment

#### Docker Compose ile
```bash
# Production deployment
./deploy/deploy.sh production

# Servisleri kontrol et
docker-compose ps

# Logları izle
docker-compose logs -f monitoring
```

#### Kubernetes ile
```bash
# Kubernetes'e deploy et
kubectl apply -f k8s/storage.yaml
kubectl apply -f k8s/deployment.yaml

# Servisleri kontrol et
kubectl get pods
kubectl get services
```
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
