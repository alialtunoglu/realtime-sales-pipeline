"""
Configuration Management for Retail Analytics Platform
Provides centralized configuration for all components
"""
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from pathlib import Path

@dataclass
class SparkConfig:
    """Spark configuration settings"""
    app_name: str = "RetailAnalyticsPlatform"
    master: str = "local[*]"
    executor_memory: str = "4g"
    driver_memory: str = "2g"
    executor_cores: str = "2"
    max_result_size: str = "2g"
    
    # Delta Lake specific configs
    delta_extensions: str = "io.delta.sql.DeltaSparkSessionExtension"
    delta_catalog: str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    
    # Performance tuning
    adaptive_enabled: bool = True
    adaptive_coalesce_partitions: bool = True
    serializer: str = "org.apache.spark.serializer.KryoSerializer"

@dataclass
class StorageConfig:
    """Storage paths and configuration"""
    project_root: str = field(default_factory=lambda: str(Path(__file__).parent.parent))
    data_input_path: str = "data/input"
    data_output_path: str = "data/output"
    
    # Delta Lake paths
    bronze_path: str = "delta/bronze"
    silver_path: str = "delta/silver"
    gold_path: str = "delta/gold"
    checkpoint_location: str = "checkpoints"
    
    # Logging
    log_path: str = "logs"
    
    def get_absolute_path(self, relative_path: str) -> str:
        """Convert relative path to absolute path"""
        return os.path.join(self.project_root, relative_path)

@dataclass
class AirflowConfig:
    """Airflow configuration settings"""
    home: str = field(default_factory=lambda: os.path.join(Path(__file__).parent.parent, "airflow"))
    dags_folder: str = "airflow/dags"
    webserver_port: int = 8080
    scheduler_heartbeat_sec: int = 5
    
    # DAG default args
    owner: str = "data_engineer"
    depends_on_past: bool = False
    email_on_failure: bool = False
    email_on_retry: bool = False
    retries: int = 3
    retry_delay_minutes: int = 5

@dataclass
class DashboardConfig:
    """Dashboard configuration"""
    port: int = 8501
    host: str = "localhost"
    title: str = "Retail Analytics Dashboard"
    page_title: str = "📊 Retail Analytics"
    page_icon: str = "📊"
    layout: str = "wide"
    initial_sidebar_state: str = "expanded"

@dataclass
class DatabaseConfig:
    """Database configuration (for future use)"""
    host: str = "localhost"
    port: int = 5432
    database: str = "retail_analytics"
    username: str = "admin"
    password: str = "password"
    
    @property
    def connection_string(self) -> str:
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"

@dataclass
class LoggingConfig:
    """Logging configuration"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format: str = "%Y-%m-%d %H:%M:%S"
    max_file_size: int = 10 * 1024 * 1024  # 10MB
    backup_count: int = 5

class Config:
    """Main configuration class"""
    
    def __init__(self, environment: str = "development"):
        self.environment = environment
        self.spark = SparkConfig()
        self.storage = StorageConfig()
        self.airflow = AirflowConfig()
        self.dashboard = DashboardConfig()
        self.database = DatabaseConfig()
        self.logging = LoggingConfig()
        
        # Apply environment-specific overrides
        self._apply_environment_config()
    
    def _apply_environment_config(self):
        """Apply environment-specific configuration overrides"""
        
        if self.environment == "production":
            # Production optimizations
            self.spark.master = "spark://spark-master:7077"
            self.spark.executor_memory = "8g"
            self.spark.driver_memory = "4g"
            self.logging.level = "WARNING"
            self.dashboard.host = "0.0.0.0"
            
        elif self.environment == "testing":
            # Testing configurations
            self.spark.master = "local[1]"
            self.spark.executor_memory = "1g"
            self.spark.driver_memory = "512m"
            self.logging.level = "DEBUG"
            self.airflow.retries = 1
            
        elif self.environment == "development":
            # Development configurations (default)
            self.logging.level = "DEBUG"
            self.dashboard.port = 8504  # Avoid conflicts
    
    @classmethod
    def from_env(cls) -> 'Config':
        """Create configuration from environment variables"""
        environment = os.getenv("ENV", "development")
        config = cls(environment)
        
        # Override with environment variables if present
        if os.getenv("SPARK_MASTER"):
            config.spark.master = os.getenv("SPARK_MASTER")
        
        if os.getenv("SPARK_EXECUTOR_MEMORY"):
            config.spark.executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY")
        
        if os.getenv("PROJECT_ROOT"):
            config.storage.project_root = os.getenv("PROJECT_ROOT")
        
        if os.getenv("DASHBOARD_PORT"):
            config.dashboard.port = int(os.getenv("DASHBOARD_PORT"))
        
        return config
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary"""
        return {
            "environment": self.environment,
            "spark": self.spark.__dict__,
            "storage": self.storage.__dict__,
            "airflow": self.airflow.__dict__,
            "dashboard": self.dashboard.__dict__,
            "database": self.database.__dict__,
            "logging": self.logging.__dict__
        }

# Global configuration instance
config = Config.from_env()
