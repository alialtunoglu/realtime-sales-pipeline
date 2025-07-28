"""
Bronze Layer ETL - Data Ingestion
Ingests raw data from CSV to Delta Lake Bronze layer
"""
import time
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, lit

from config.settings import config
from src.utils.logger import get_logger, log_execution_time
from src.utils.spark_factory import get_spark

logger = get_logger(__name__)


class BronzeETL:
    """Bronze layer ETL operations"""
    
    def __init__(self):
        self.spark = get_spark()
        self.config = config
        self.logger = logger  # Add logger instance for testing
    
    @log_execution_time(logger)
    def extract_from_csv(self, file_path: str) -> DataFrame:
        """
        Extract data from CSV file
        
        Args:
            file_path: Path to CSV file
            
        Returns:
            DataFrame with raw data
        """
        logger.info(f"📥 Extracting data from CSV: {file_path}")
        
        # Construct full path
        full_path = self.config.storage.get_absolute_path(file_path)
        
        try:
            df = self.spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(full_path)
            
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully extracted {row_count:,} rows from CSV",
                file_path=file_path,
                row_count=row_count,
                schema=str(df.schema)
            )
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to extract from CSV: {str(e)}", file_path=file_path)
            raise
    
    @log_execution_time(logger)
    def transform_bronze(self, df: DataFrame) -> DataFrame:
        """
        Apply basic transformations for Bronze layer
        
        Args:
            df: Raw DataFrame
            
        Returns:
            DataFrame with Bronze transformations
        """
        logger.info("🔄 Applying Bronze layer transformations")
        
        # Add ingestion metadata
        df_bronze = df.withColumn("ingestion_timestamp", current_timestamp())
        
        # Add source file path if metadata exists (for real file reads)
        if "_metadata" in [field.name for field in df.schema.fields]:
            df_bronze = df_bronze.withColumn("source_file", col("_metadata.file_path"))
        else:
            # For test data or manual DataFrames, use a default source
            df_bronze = df_bronze.withColumn("source_file", lit("manual_input"))
        
        # Log schema info
        logger.info(
            "📋 Bronze schema applied",
            column_count=len(df_bronze.columns),
            columns=df_bronze.columns
        )
        
        return df_bronze
    
    @log_execution_time(logger)
    def load_to_bronze(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Load DataFrame to Bronze Delta table
        
        Args:
            df: DataFrame to load
            table_name: Name of the Bronze table
            mode: Write mode (overwrite, append)
        """
        bronze_path = f"{self.config.storage.bronze_path}/{table_name}"
        full_path = self.config.storage.get_absolute_path(bronze_path)
        
        logger.info(f"💾 Loading to Bronze table: {table_name}")
        
        try:
            df.write.format("delta") \
              .mode(mode) \
              .option("overwriteSchema", "true") \
              .save(full_path)
            
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully loaded {row_count:,} rows to Bronze",
                table_name=table_name,
                path=full_path,
                mode=mode,
                row_count=row_count
            )
            
            # Log data quality metrics
            self._log_data_quality_metrics(df, table_name)
            
        except Exception as e:
            logger.error(f"❌ Failed to load to Bronze: {str(e)}", table_name=table_name)
            raise
    
    def _log_data_quality_metrics(self, df: DataFrame, table_name: str):
        """Log data quality metrics for Bronze table"""
        try:
            total_rows = df.count()
            null_counts = {}
            
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
            
            quality_metrics = {
                "total_rows": total_rows,
                "null_counts": null_counts,
                "completeness_score": 1.0 - (sum(null_counts.values()) / (total_rows * len(df.columns)))
            }
            
            logger.log_data_quality(table_name, quality_metrics)
            
        except Exception as e:
            logger.warning(f"⚠️ Could not collect data quality metrics: {str(e)}")
    
    @log_execution_time(logger)
    def run_bronze_ingestion(self, input_file: str = "data/input/retail_data.csv", 
                           table_name: str = "online_retail") -> None:
        """
        Complete Bronze ingestion pipeline
        
        Args:
            input_file: Input CSV file path
            table_name: Target Bronze table name
        """
        pipeline_start = time.time()
        
        logger.log_pipeline_start(
            "bronze_ingestion",
            input_file=input_file,
            table_name=table_name
        )
        
        try:
            # Extract
            df_raw = self.extract_from_csv(input_file)
            
            # Transform
            df_bronze = self.transform_bronze(df_raw)
            
            # Load
            self.load_to_bronze(df_bronze, table_name)
            
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "bronze_ingestion",
                duration=duration,
                status="success",
                input_file=input_file,
                table_name=table_name,
                row_count=df_bronze.count()
            )
            
        except Exception as e:
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "bronze_ingestion",
                duration=duration,
                status="failed",
                error=str(e)
            )
            raise


def run_bronze_ingestion():
    """Main function for Bronze ingestion (backward compatibility)"""
    bronze_etl = BronzeETL()
    bronze_etl.run_bronze_ingestion()


if __name__ == "__main__":
    run_bronze_ingestion()
