# 🚀 Getting Started Guide

## 📋 Prerequisites

### System Requirements
- **Operating System**: macOS, Linux, or Windows 10+
- **Python**: 3.10 or higher
- **Memory**: 8GB RAM minimum, 16GB recommended
- **Storage**: 5GB free space for data and logs

### Required Software
- **Git**: For version control
- **Conda/pip**: For Python package management
- **Java 8/11**: Required for Apache Spark

## 🛠️ Installation

### 1. Clone Repository
```bash
git clone https://github.com/yourusername/realtime-sales-pipeline.git
cd realtime-sales-pipeline
```

### 2. Setup Python Environment
```bash
# Using Conda (Recommended)
conda create -n retail-analytics python=3.10
conda activate retail-analytics

# Or using venv
python -m venv retail-analytics
source retail-analytics/bin/activate  # Linux/Mac
# retail-analytics\Scripts\activate  # Windows
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Verify Installation
```bash
python -c "import pyspark; print('Spark installed successfully')"
python -c "import delta; print('Delta Lake installed successfully')"
python -c "import airflow; print('Airflow installed successfully')"
```

## ⚙️ Configuration

### 1. Environment Setup
```bash
# Set environment variables
export ENV=development
export PYTHONPATH="${PYTHONPATH}:$(pwd)"

# For production
export ENV=production
export SPARK_MASTER=spark://your-cluster:7077
```

### 2. Airflow Setup
```bash
# Initialize Airflow database
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 3. Data Setup
```bash
# Ensure data directory exists
mkdir -p data/input
mkdir -p data/output

# Download sample data (if not present)
# Place your retail_data.csv in data/input/
```

## 🏃 Quick Start

### Option 1: Run Individual Components

#### Bronze Ingestion
```bash
python src/etl/bronze_ingestion.py
```

#### Silver Cleaning
```bash
python tasks/clean_data.py
```

#### Gold Aggregation
```bash
python tasks/generate_gold.py
```

#### Advanced Analytics
```bash
python tasks/advanced_analytics.py
```

### Option 2: Use Automation Scripts
```bash
# Quick test
./test_pipeline.sh

# Complete pipeline
./run_complete_pipeline.sh
```

### Option 3: Airflow Orchestration
```bash
# Start Airflow services
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080 &
airflow scheduler &

# Trigger pipeline
airflow dags trigger retail_sales_pipeline

# Access Web UI
open http://localhost:8080
```

### Option 4: Dashboard
```bash
# Start Streamlit dashboard
streamlit run dashboard/app.py --server.port 8501

# Access dashboard
open http://localhost:8501
```

## 📊 Sample Workflow

### Complete End-to-End Example
```bash
# 1. Activate environment
conda activate retail-analytics

# 2. Run complete pipeline
./run_complete_pipeline.sh

# 3. Start dashboard
streamlit run dashboard/app.py --server.port 8501 &

# 4. Start Airflow (optional)
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080 &
airflow scheduler &

# 5. View results
echo "Dashboard: http://localhost:8501"
echo "Airflow: http://localhost:8080"
```

## 🧪 Testing

### Run Tests
```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests  
pytest tests/integration/ -v

# All tests
pytest tests/ -v --cov=src
```

### Data Quality Checks
```bash
# Run data quality validation
python tests/data_quality/validate_pipeline.py
```

## 🐛 Troubleshooting

### Common Issues

#### Java/Spark Issues
```bash
# Check Java version
java -version

# Set JAVA_HOME if needed
export JAVA_HOME=/path/to/java

# For macOS with Homebrew
export JAVA_HOME=/opt/homebrew/opt/openjdk@11
```

#### Memory Issues
```bash
# Reduce Spark memory if needed
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g
```

#### Port Conflicts
```bash
# Use different ports
streamlit run dashboard/app.py --server.port 8502
airflow webserver --port 8081
```

#### Permission Issues
```bash
# Make scripts executable
chmod +x *.sh
chmod +x scripts/*.sh
```

### Getting Help

#### Check Logs
```bash
# View pipeline logs
tail -f logs/bronze_ingestion_$(date +%Y%m%d).log

# View Airflow logs
tail -f airflow/logs/scheduler/latest
```

#### Common Commands
```bash
# Reset pipeline
rm -rf delta/silver delta/gold
python src/etl/bronze_ingestion.py

# Clear Airflow cache
airflow dags delete retail_sales_pipeline
airflow db reset
```

## 📚 Next Steps

### Development
1. **Explore the Code**: Start with `src/etl/` modules
2. **Run Tests**: Ensure everything works
3. **Check Logs**: Review `logs/` directory
4. **Customize**: Modify configuration in `config/`

### Production Deployment
1. **Docker Setup**: Use provided Dockerfile
2. **Cloud Deployment**: Follow cloud-specific guides
3. **Monitoring**: Set up alerting and monitoring
4. **Scaling**: Configure cluster resources

### Learning
1. **Documentation**: Read `docs/` directory
2. **Notebooks**: Explore Jupyter notebooks
3. **Architecture**: Understand `docs/architecture.md`
4. **Best Practices**: Review code patterns
