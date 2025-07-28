"""
Silver Layer ETL Operations
Handles data cleaning, validation, and standardization
"""
import time
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, isnan, isnull, trim, upper, regexp_replace,
    to_timestamp, year, month, dayofmonth, dayofweek,
    abs as spark_abs, mean, stddev, percentile_approx
)
from pyspark.sql.types import DoubleType, IntegerType

from config.settings import config
from src.utils.logger import logger, log_execution_time
from src.utils.spark_factory import SparkFactory


class SilverETL:
    """Silver layer ETL operations for data cleaning and validation"""
    
    def __init__(self):
        """Initialize Silver ETL with Spark session and configuration"""
        self.spark = SparkFactory.get_spark_session()
        self.config = config
        self.logger = logger  # For testing purposes
        
    def read_from_bronze(self, table_name: str = "online_retail") -> DataFrame:
        """
        Read data from Bronze layer
        
        Args:
            table_name: Bronze table name to read from
            
        Returns:
            DataFrame from Bronze layer
        """
        bronze_path = f"{self.config.storage.bronze_path}/{table_name}"
        full_path = self.config.storage.get_absolute_path(bronze_path)
        
        logger.info(f"📖 Reading from Bronze table: {table_name}")
        
        try:
            df = self.spark.read.format("delta").load(full_path)
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully read {row_count:,} rows from Bronze",
                table_name=table_name,
                path=full_path,
                row_count=row_count
            )
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to read from Bronze: {str(e)}", table_name=table_name)
            raise
    
    def clean_invoice_data(self, df: DataFrame) -> DataFrame:
        """
        Clean and validate invoice data
        
        Args:
            df: Raw DataFrame from Bronze layer
            
        Returns:
            Cleaned DataFrame
        """
        logger.info("🧹 Starting invoice data cleaning")
        
        original_count = df.count()
        
        # Remove rows with null InvoiceNo or negative quantities
        df_cleaned = df.filter(
            (col("InvoiceNo").isNotNull()) & 
            (col("InvoiceNo") != "") &
            (col("Quantity") > 0) &
            (col("UnitPrice") > 0)
        )
        
        # Clean string columns
        df_cleaned = df_cleaned.withColumn(
            "Description", 
            trim(upper(col("Description")))
        ).withColumn(
            "Country",
            trim(upper(col("Country")))
        ).withColumn(
            "StockCode",
            trim(upper(col("StockCode")))
        )
        
        # Parse invoice date
        df_cleaned = df_cleaned.withColumn(
            "InvoiceTimestamp",
            to_timestamp(col("InvoiceDate"), "M/d/yyyy H:mm")
        )
        
        # Add date components for analytics
        df_cleaned = df_cleaned.withColumn("Year", year(col("InvoiceTimestamp"))) \
                              .withColumn("Month", month(col("InvoiceTimestamp"))) \
                              .withColumn("Day", dayofmonth(col("InvoiceTimestamp"))) \
                              .withColumn("DayOfWeek", dayofweek(col("InvoiceTimestamp")))
        
        # Calculate total amount
        df_cleaned = df_cleaned.withColumn(
            "TotalAmount",
            col("Quantity") * col("UnitPrice")
        )
        
        cleaned_count = df_cleaned.count()
        removed_count = original_count - cleaned_count
        
        logger.info(
            f"✅ Completed data cleaning: {cleaned_count:,} rows retained, {removed_count:,} rows removed",
            original_count=original_count,
            cleaned_count=cleaned_count,
            removed_count=removed_count,
            retention_rate=round((cleaned_count / original_count) * 100, 2)
        )
        
        return df_cleaned
    
    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        """
        Validate data quality and return metrics
        
        Args:
            df: DataFrame to validate
            
        Returns:
            Dictionary with data quality metrics
        """
        logger.info("🔍 Starting data quality validation")
        
        total_rows = df.count()
        
        # Check for outliers in quantities
        quantity_stats = df.select(
            mean(col("Quantity")).alias("mean_qty"),
            stddev(col("Quantity")).alias("stddev_qty")
        ).collect()[0]
        
        mean_qty = quantity_stats["mean_qty"]
        stddev_qty = quantity_stats["stddev_qty"]
        
        # Define outlier thresholds (3 standard deviations)
        upper_threshold = mean_qty + (3 * stddev_qty)
        lower_threshold = max(0, mean_qty - (3 * stddev_qty))
        
        outlier_count = df.filter(
            (col("Quantity") > upper_threshold) | 
            (col("Quantity") < lower_threshold)
        ).count()
        
        # Check data completeness
        null_counts = {}
        for column in df.columns:
            if column not in ["CustomerID"]:  # CustomerID can be null for guest purchases
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
        
        # Calculate data quality score
        total_null_count = sum(null_counts.values())
        completeness_score = ((total_rows * len(null_counts) - total_null_count) / 
                             (total_rows * len(null_counts))) * 100
        
        outlier_percentage = (outlier_count / total_rows) * 100
        
        quality_metrics = {
            "total_rows": total_rows,
            "null_counts": null_counts,
            "outlier_count": outlier_count,
            "outlier_percentage": round(outlier_percentage, 2),
            "completeness_score": round(completeness_score, 2),
            "mean_quantity": round(mean_qty, 2),
            "stddev_quantity": round(stddev_qty, 2)
        }
        
        logger.info(
            f"✅ Data quality validation completed",
            **quality_metrics
        )
        
        return quality_metrics
    
    def apply_business_rules(self, df: DataFrame) -> DataFrame:
        """
        Apply business rules for data standardization
        
        Args:
            df: DataFrame to apply rules to
            
        Returns:
            DataFrame with business rules applied
        """
        logger.info("📋 Applying business rules")
        
        # Filter out test transactions and returns
        df_filtered = df.filter(
            ~col("InvoiceNo").startswith("C") &  # Remove credit notes/returns
            ~col("Description").contains("TEST") &
            ~col("Description").contains("SAMPLE") &
            ~col("Description").contains("ADJUSTMENT")
        )
        
        # Standardize country names
        df_filtered = df_filtered.withColumn(
            "Country",
            when(col("Country").isin("EIRE", "IRELAND"), "IRELAND")
            .when(col("Country") == "EUROPEAN COMMUNITY", "EUROPE")
            .when(col("Country") == "UNSPECIFIED", "UNKNOWN")
            .otherwise(col("Country"))
        )
        
        # Add customer segment flag
        df_filtered = df_filtered.withColumn(
            "CustomerType",
            when(col("CustomerID").isNull(), "GUEST")
            .otherwise("REGISTERED")
        )
        
        original_count = df.count()
        filtered_count = df_filtered.count()
        
        logger.info(
            f"✅ Business rules applied: {filtered_count:,} rows retained from {original_count:,}",
            original_count=original_count,
            filtered_count=filtered_count,
            filtered_percentage=round((filtered_count / original_count) * 100, 2)
        )
        
        return df_filtered
    
    def write_to_silver(self, df: DataFrame, table_name: str = "online_retail_cleaned", 
                       mode: str = "overwrite") -> None:
        """
        Write cleaned data to Silver layer
        
        Args:
            df: Cleaned DataFrame to write
            table_name: Silver table name
            mode: Write mode (overwrite, append)
        """
        silver_path = f"{self.config.storage.silver_path}/{table_name}"
        full_path = self.config.storage.get_absolute_path(silver_path)
        
        logger.info(f"💾 Writing to Silver table: {table_name}")
        
        try:
            df.write.format("delta") \
              .mode(mode) \
              .option("overwriteSchema", "true") \
              .save(full_path)
            
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully wrote {row_count:,} rows to Silver",
                table_name=table_name,
                path=full_path,
                mode=mode,
                row_count=row_count
            )
            
            # Log data quality metrics for Silver data
            self._log_silver_quality_metrics(df, table_name)
            
        except Exception as e:
            logger.error(f"❌ Failed to write to Silver: {str(e)}", table_name=table_name)
            raise
    
    def _log_silver_quality_metrics(self, df: DataFrame, table_name: str) -> None:
        """
        Log data quality metrics for Silver layer
        
        Args:
            df: DataFrame to analyze
            table_name: Table name for logging context
        """
        try:
            quality_metrics = self.validate_data_quality(df)
            self.logger.log_data_quality(table_name, quality_metrics)
            
        except Exception as e:
            logger.warning(f"⚠️ Could not collect Silver quality metrics: {str(e)}")
    
    @log_execution_time(logger)
    def run_silver_cleaning(self, bronze_table: str = "online_retail",
                           silver_table: str = "online_retail_cleaned") -> None:
        """
        Complete Silver cleaning pipeline
        
        Args:
            bronze_table: Source Bronze table name
            silver_table: Target Silver table name
        """
        pipeline_start = time.time()
        
        logger.log_pipeline_start(
            "silver_cleaning",
            bronze_table=bronze_table,
            silver_table=silver_table
        )
        
        try:
            # Read from Bronze
            df_bronze = self.read_from_bronze(bronze_table)
            
            # Clean data
            df_cleaned = self.clean_invoice_data(df_bronze)
            
            # Apply business rules
            df_silver = self.apply_business_rules(df_cleaned)
            
            # Write to Silver
            self.write_to_silver(df_silver, silver_table)
            
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "silver_cleaning",
                duration=duration,
                status="success",
                bronze_table=bronze_table,
                silver_table=silver_table,
                row_count=df_silver.count()
            )
            
        except Exception as e:
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "silver_cleaning",
                duration=duration,
                status="failed",
                error=str(e)
            )
            raise
