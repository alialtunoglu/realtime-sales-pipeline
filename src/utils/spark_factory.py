"""
Spark Session Factory
Provides centralized Spark session creation with proper configuration
"""
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from typing import Optional

from config.settings import config
from src.utils.logger import PipelineLogger

logger = PipelineLogger(__name__)


class SparkSessionFactory:
    """Factory class for creating Spark sessions with proper configuration"""
    
    _instance: Optional[SparkSession] = None
    
    @classmethod
    def get_spark_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """
        Get or create Spark session with Delta Lake configuration
        
        Args:
            app_name: Optional application name override
            
        Returns:
            Configured Spark session
        """
        if cls._instance is None or cls._instance._jsc.sc().isStopped():
            cls._instance = cls._create_spark_session(app_name)
        
        return cls._instance
    
    @classmethod
    def _create_spark_session(cls, app_name: Optional[str] = None) -> SparkSession:
        """Create new Spark session with configuration"""
        
        app_name = app_name or config.spark.app_name
        
        logger.info(f"🔥 Creating Spark session: {app_name}")
        
        # Build Spark session with configuration
        builder = SparkSession.builder \
            .appName(app_name) \
            .master(config.spark.master) \
            .config("spark.executor.memory", config.spark.executor_memory) \
            .config("spark.driver.memory", config.spark.driver_memory) \
            .config("spark.executor.cores", config.spark.executor_cores) \
            .config("spark.driver.maxResultSize", config.spark.max_result_size) \
            .config("spark.serializer", config.spark.serializer) \
            .config("spark.sql.adaptive.enabled", str(config.spark.adaptive_enabled)) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", str(config.spark.adaptive_coalesce_partitions)) \
            .config("spark.sql.extensions", config.spark.delta_extensions) \
            .config("spark.sql.catalog.spark_catalog", config.spark.delta_catalog)
        
        # Configure Delta Lake
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        
        # Set log level
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info(
            f"✅ Spark session created successfully",
            app_name=app_name,
            master=config.spark.master,
            executor_memory=config.spark.executor_memory,
            driver_memory=config.spark.driver_memory
        )
        
        return spark
    
    @classmethod
    def stop_session(cls):
        """Stop current Spark session"""
        if cls._instance is not None:
            logger.info("🛑 Stopping Spark session")
            cls._instance.stop()
            cls._instance = None


def get_spark() -> SparkSession:
    """
    Convenience function to get Spark session
    
    Returns:
        Configured Spark session
    """
    return SparkSessionFactory.get_spark_session()


# Alias for backward compatibility
SparkFactory = SparkSessionFactory
