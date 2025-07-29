# Docker Configuration for Production Deployment
# Multi-stage Docker build for optimized production deployment

# Stage 1: Base image with system dependencies
FROM python:3.10-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    default-jdk \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Stage 2: Dependencies installation
FROM base as dependencies

# Copy requirements and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Stage 3: Application
FROM dependencies as application

# Create app user for security
RUN useradd --create-home --shell /bin/bash app

# Set work directory
WORKDIR /app

# Copy application code
COPY --chown=app:app . /app/

# Create necessary directories
RUN mkdir -p /app/logs /app/data /app/delta /app/checkpoints \
    && chown -R app:app /app

# Switch to app user
USER app

# Expose ports
EXPOSE 8080 8501 8793

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8501')" || exit 1

# Default command
CMD ["python", "-m", "streamlit", "run", "dashboard/app.py", "--server.port=8501", "--server.address=0.0.0.0"]
