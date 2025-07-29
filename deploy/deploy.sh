#!/bin/bash
# Production Deployment Script
# Automates the deployment process for the pipeline

set -e  # Exit on any error

# Configuration
ENVIRONMENT=${1:-production}
IMAGE_TAG=${2:-latest}
REGISTRY="ghcr.io/alialtunoglu/realtime-sales-pipeline"

echo "🚀 Starting deployment to $ENVIRONMENT environment"
echo "📦 Using image tag: $IMAGE_TAG"

# Function to log with timestamp
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Pre-deployment checks
log "🔍 Running pre-deployment checks..."

# Check if Docker is installed
if ! command_exists docker; then
    log "❌ Docker is not installed"
    exit 1
fi

# Check if Docker Compose is installed
if ! command_exists docker-compose; then
    log "❌ Docker Compose is not installed"
    exit 1
fi

# Check if required environment variables are set
if [ -z "$AIRFLOW_ADMIN_PASSWORD" ]; then
    log "⚠️  AIRFLOW_ADMIN_PASSWORD not set, using default"
    export AIRFLOW_ADMIN_PASSWORD="admin"
fi

log "✅ Pre-deployment checks passed"

# Pull latest images
log "📥 Pulling latest Docker images..."
docker pull $REGISTRY:$IMAGE_TAG || {
    log "❌ Failed to pull image $REGISTRY:$IMAGE_TAG"
    exit 1
}

# Stop existing services
log "🛑 Stopping existing services..."
docker-compose down --remove-orphans || log "⚠️  No existing services to stop"

# Backup data (if production)
if [ "$ENVIRONMENT" = "production" ]; then
    log "💾 Creating data backup..."
    
    BACKUP_DIR="./backups/$(date +%Y%m%d_%H%M%S)"
    mkdir -p "$BACKUP_DIR"
    
    # Backup data directory
    if [ -d "./data" ]; then
        cp -r ./data "$BACKUP_DIR/"
        log "✅ Data backed up to $BACKUP_DIR"
    fi
    
    # Backup Delta Lake data
    if [ -d "./delta" ]; then
        cp -r ./delta "$BACKUP_DIR/"
        log "✅ Delta Lake data backed up"
    fi
    
    # Backup Airflow database
    if [ -f "./airflow/airflow.db" ]; then
        cp ./airflow/airflow.db "$BACKUP_DIR/"
        log "✅ Airflow database backed up"
    fi
fi

# Deploy services
log "🚀 Deploying services..."

# Set environment-specific configurations
export PIPELINE_ENV=$ENVIRONMENT
export IMAGE_TAG=$IMAGE_TAG

# Deploy with Docker Compose
docker-compose up -d --build

# Wait for services to be healthy
log "⏳ Waiting for services to be healthy..."
sleep 30

# Health checks
log "🏥 Running health checks..."

# Check dashboard health
if curl -f http://localhost:8501 >/dev/null 2>&1; then
    log "✅ Dashboard is healthy"
else
    log "❌ Dashboard health check failed"
    exit 1
fi

# Check Airflow health
if curl -f http://localhost:8080/health >/dev/null 2>&1; then
    log "✅ Airflow is healthy"
else
    log "⚠️  Airflow health check failed (may still be starting)"
fi

# Check monitoring service
if docker-compose ps monitoring | grep -q "Up"; then
    log "✅ Monitoring service is running"
else
    log "❌ Monitoring service is not running"
    exit 1
fi

# Run monitoring health check
log "🔍 Running monitoring system health check..."
docker-compose exec -T monitoring python monitoring.py run

# Display service status
log "📊 Service Status:"
docker-compose ps

# Display URLs
log "🌐 Application URLs:"
log "   Dashboard: http://localhost:8501"
log "   Airflow: http://localhost:8080 (admin/admin)"
log "   Monitoring: Check logs with 'docker-compose logs monitoring'"

# Post-deployment tasks
log "📝 Running post-deployment tasks..."

# Trigger initial data pipeline (if in production)
if [ "$ENVIRONMENT" = "production" ]; then
    log "🔄 Triggering initial data pipeline..."
    # Wait a bit more for Airflow to be fully ready
    sleep 60
    
    # Trigger Bronze ingestion DAG
    docker-compose exec -T airflow-webserver airflow dags trigger bronze_ingestion_dag || log "⚠️  Failed to trigger Bronze DAG"
    
    # Trigger monitoring DAG
    docker-compose exec -T airflow-webserver airflow dags trigger monitoring_pipeline || log "⚠️  Failed to trigger monitoring DAG"
fi

# Success notification
log "🎉 Deployment completed successfully!"
log "📈 Monitor the system with: docker-compose logs -f monitoring"
log "🔧 Manage with: docker-compose [start|stop|restart] [service]"

# Cleanup old images (keep last 3 versions)
log "🧹 Cleaning up old Docker images..."
docker image prune -f
docker images | grep "$REGISTRY" | sort -k2 -r | tail -n +4 | awk '{print $3}' | xargs -r docker rmi || log "⚠️  No old images to clean"

log "✅ Deployment script completed"
