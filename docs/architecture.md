# 🏗️ Architecture Documentation

## 📊 System Architecture

### High-Level Architecture
```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │   Consumption   │
│                 │    │                 │    │                 │
│ ┌─────────────┐ │    │ ┌─────────────┐ │    │ ┌─────────────┐ │
│ │ CSV Files   │ │    │ │Apache Spark │ │    │ │ Streamlit   │ │
│ │ API Data    │ ├────┤ │Delta Lake   │ ├────┤ │ Dashboard   │ │
│ │ Streaming   │ │    │ │Airflow      │ │    │ │ BI Tools    │ │
│ └─────────────┘ │    │ └─────────────┘ │    │ └─────────────┘ │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Medallion Architecture
```
🥉 Bronze Layer (Raw Data)
├── online_retail/          # Raw CSV data in Delta format
├── customer_data/          # Customer information
└── product_catalog/        # Product master data

🥈 Silver Layer (Cleaned Data)  
├── online_retail_cleaned/  # Quality-validated transactions
├── customer_enriched/      # Enriched customer profiles
└── product_normalized/     # Standardized product data

🥇 Gold Layer (Business Data)
├── daily_sales/           # Daily aggregated sales
├── top_products/          # Product performance metrics
├── country_sales/         # Geographic sales analysis
└── customer_segments/     # RFM customer segments

🔬 Advanced Analytics
├── rfm_table/            # Customer segmentation
├── cltv_table/           # Customer lifetime value
└── forecast_table/       # Revenue predictions
```

## 🔄 Data Flow

### Batch Processing Flow
1. **Data Ingestion** (Bronze ETL)
   - Extract from CSV/API sources
   - Validate basic schema
   - Store in Delta Bronze layer
   - Log ingestion metrics

2. **Data Cleaning** (Silver ETL)
   - Apply business rules
   - Remove null values
   - Standardize formats
   - Perform quality checks

3. **Data Aggregation** (Gold ETL)
   - Create business aggregations
   - Calculate KPIs
   - Generate reporting tables

4. **Advanced Analytics**
   - Customer segmentation (RFM)
   - Lifetime value calculation
   - Predictive modeling

### Real-time Processing Flow
1. **Stream Ingestion**
   - Kafka/file-based streaming
   - Schema validation
   - Real-time quality checks

2. **Stream Processing**
   - Spark Streaming
   - Window-based aggregations
   - Real-time alerting

3. **Live Dashboard Updates**
   - Real-time metrics
   - Anomaly detection
   - Business monitoring

## 🛠️ Technology Stack

### Core Processing
- **Apache Spark 3.4+**: Distributed data processing
- **Delta Lake 2.2+**: ACID transactions and time travel
- **Python 3.10+**: Primary development language

### Orchestration
- **Apache Airflow 2.8+**: Workflow orchestration
- **DAGs**: Pipeline dependency management
- **Schedulers**: Automated execution

### Storage & Data
- **Delta Lake**: Primary data storage
- **Parquet**: Columnar storage format
- **JSON**: Configuration and logging

### Visualization
- **Streamlit**: Interactive dashboards
- **Plotly**: Advanced charting
- **HTML/CSS**: Custom styling

### Development & Deployment
- **Git**: Version control
- **GitHub Actions**: CI/CD pipeline
- **Docker**: Containerization
- **pytest**: Testing framework

## 🔧 Configuration Management

### Environment Configurations
```python
# Development
ENV=development
SPARK_MASTER=local[*]
LOG_LEVEL=DEBUG

# Production  
ENV=production
SPARK_MASTER=spark://cluster:7077
LOG_LEVEL=WARNING
```

### Configuration Hierarchy
1. Default configuration (config/settings.py)
2. Environment-specific overrides
3. Environment variables
4. Runtime parameters

## 📊 Performance Characteristics

### Processing Capabilities
- **Data Volume**: 500K+ records per batch
- **Processing Time**: ~2-3 minutes for full pipeline
- **Throughput**: 5,000+ records/second
- **Memory Usage**: 4-8GB typical

### Scalability
- **Horizontal**: Add Spark workers
- **Vertical**: Increase memory/CPU
- **Storage**: Unlimited with Delta Lake
- **Concurrent Users**: 10+ dashboard users

## 🔍 Monitoring & Observability

### Metrics Collection
- Pipeline execution times
- Data quality scores
- System resource usage
- Business KPIs

### Logging Strategy
- Structured JSON logging
- Multiple log levels
- Centralized log aggregation
- Performance metrics

### Alerting
- Pipeline failures
- Data quality issues
- System resource exhaustion
- Business anomalies
