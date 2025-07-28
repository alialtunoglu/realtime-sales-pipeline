#!/bin/bash

# 🚀 Complete Development Setup Script
# Sets up the entire development environment from scratch

set -e  # Exit on any error

echo "🚀 Starting Retail Analytics Platform Setup..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Functions
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if we're in the right directory
if [ ! -f "config/settings.py" ]; then
    print_error "Please run this script from the project root directory"
    exit 1
fi

print_status "Setting up project directories..."

# Create necessary directories
mkdir -p logs
mkdir -p data/input
mkdir -p data/output
mkdir -p checkpoints
mkdir -p monitoring/metrics

# Create .gitkeep files for empty directories
touch logs/.gitkeep
touch data/input/.gitkeep
touch data/output/.gitkeep
touch checkpoints/.gitkeep
touch monitoring/.gitkeep

print_success "Project directories created"

# Check Python version
print_status "Checking Python version..."
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
required_version="3.10"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    print_warning "Python 3.10+ required. Current version: $python_version"
    print_warning "Please upgrade Python and try again"
fi

# Check if virtual environment exists
if [ ! -d "venv" ] && [ ! -d "retail-analytics" ]; then
    print_status "Creating Python virtual environment..."
    python3 -m venv retail-analytics
    print_success "Virtual environment created"
else
    print_success "Virtual environment already exists"
fi

# Activate virtual environment
print_status "Activating virtual environment..."
source retail-analytics/bin/activate

# Upgrade pip
print_status "Upgrading pip..."
pip install --upgrade pip

# Install dependencies
print_status "Installing Python dependencies..."
pip install -r requirements.txt

print_success "Dependencies installed"

# Set environment variables
print_status "Setting up environment variables..."
export PYTHONPATH="${PYTHONPATH}:$(pwd)"
export ENV=development

# Initialize Airflow
print_status "Initializing Airflow..."
export AIRFLOW_HOME=$(pwd)/airflow

# Check if Airflow is already initialized
if [ ! -f "airflow/airflow.db" ]; then
    airflow db init
    print_success "Airflow database initialized"
    
    # Create admin user
    print_status "Creating Airflow admin user..."
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin
    
    print_success "Airflow admin user created (admin/admin)"
else
    print_success "Airflow already initialized"
fi

# Run basic tests
print_status "Running basic validation tests..."

# Test configuration
python3 -c "
from config.settings import config
print('✅ Configuration loaded successfully')
print(f'Environment: {config.environment}')
print(f'Project root: {config.storage.project_root}')
"

# Test Spark session
python3 -c "
from src.utils.spark_factory import get_spark
spark = get_spark()
print('✅ Spark session created successfully')
print(f'Spark version: {spark.version}')
spark.stop()
"

# Test logging
python3 -c "
from src.utils.logger import get_logger
logger = get_logger('setup_test')
logger.info('✅ Logging system working')
print('✅ Logging configured successfully')
"

print_success "Basic validation tests passed"

# Create development environment file
print_status "Creating development environment file..."
cat > .env << EOF
# Development Environment Configuration
ENV=development
PYTHONPATH=$(pwd)
AIRFLOW_HOME=$(pwd)/airflow

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_DRIVER_MEMORY=2g
SPARK_EXECUTOR_MEMORY=4g

# Dashboard Configuration
DASHBOARD_PORT=8501

# Logging Configuration
LOG_LEVEL=DEBUG
EOF

print_success "Environment file created (.env)"

# Make scripts executable
print_status "Making scripts executable..."
chmod +x scripts/*.sh 2>/dev/null || true
chmod +x *.sh 2>/dev/null || true

print_success "Scripts made executable"

# Display setup summary
echo
echo "🎉 Setup completed successfully!"
echo
echo "📋 Summary:"
echo "  • Python virtual environment: retail-analytics"
echo "  • Dependencies installed: $(pip list | wc -l) packages"
echo "  • Airflow initialized: airflow/airflow.db"
echo "  • Environment configured: .env"
echo
echo "🚀 Quick Start Commands:"
echo "  • Activate environment: source retail-analytics/bin/activate"
echo "  • Run pipeline: ./scripts/run_pipeline.sh"
echo "  • Start dashboard: streamlit run dashboard/app.py --server.port 8501"
echo "  • Start Airflow: airflow webserver --port 8080 & airflow scheduler"
echo "  • Run tests: pytest tests/ -v"
echo
echo "🌐 Access URLs:"
echo "  • Dashboard: http://localhost:8501"
echo "  • Airflow UI: http://localhost:8080 (admin/admin)"
echo
echo "📚 Next steps:"
echo "  1. Review docs/getting-started.md"
echo "  2. Place your data in data/input/"
echo "  3. Run ./scripts/test_pipeline.sh"
echo
print_success "Ready to start developing! 🚀"
