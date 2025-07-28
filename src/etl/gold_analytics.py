"""
Gold Layer ETL Operations
Handles business metrics, aggregations, and analytics tables
"""
import time
from typing import Optional, Dict, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, sum as spark_sum, count, avg, max as spark_max, min as spark_min,
    when, datediff, lit, desc, asc, rank, dense_rank, row_number,
    collect_list, concat_ws, round as spark_round, stddev, percentile_approx,
    window, date_trunc, last_day, date_sub, months_between,
    regexp_replace, split, coalesce, countDistinct
)
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, IntegerType

from config.settings import config
from src.utils.logger import logger, log_execution_time
from src.utils.spark_factory import SparkFactory


class GoldETL:
    """Gold layer ETL operations for business analytics and metrics"""
    
    def __init__(self):
        """Initialize Gold ETL with Spark session and configuration"""
        self.spark = SparkFactory.get_spark_session()
        self.config = config
        self.logger = logger  # For testing purposes
        
    def read_from_silver(self, table_name: str = "online_retail_cleaned") -> DataFrame:
        """
        Read data from Silver layer
        
        Args:
            table_name: Silver table name to read from
            
        Returns:
            DataFrame from Silver layer
        """
        silver_path = f"{self.config.storage.silver_path}/{table_name}"
        full_path = self.config.storage.get_absolute_path(silver_path)
        
        logger.info(f"📖 Reading from Silver table: {table_name}")
        
        try:
            df = self.spark.read.format("delta").load(full_path)
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully read {row_count:,} rows from Silver",
                table_name=table_name,
                path=full_path,
                row_count=row_count
            )
            
            return df
            
        except Exception as e:
            logger.error(f"❌ Failed to read from Silver: {str(e)}", table_name=table_name)
            raise
    
    def create_daily_sales_summary(self, df: DataFrame) -> DataFrame:
        """
        Create daily sales summary for trend analysis
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Daily sales summary DataFrame
        """
        logger.info("📊 Creating daily sales summary")
        
        daily_sales = df.groupBy("Year", "Month", "Day") \
                       .agg(
                           spark_sum("TotalAmount").alias("daily_revenue"),
                           countDistinct("InvoiceNo").alias("transaction_count"),
                           count("CustomerID").alias("customer_interactions"),
                           countDistinct("CustomerID").alias("unique_customers"),
                           avg("TotalAmount").alias("avg_order_value"),
                           spark_max("TotalAmount").alias("max_order_value"),
                           spark_min("TotalAmount").alias("min_order_value")
                       ) \
                       .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2)) \
                       .withColumn("daily_revenue", spark_round(col("daily_revenue"), 2)) \
                       .orderBy("Year", "Month", "Day")
        
        row_count = daily_sales.count()
        logger.info(f"✅ Created daily sales summary with {row_count:,} days")
        
        return daily_sales
    
    def create_country_sales_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create country-wise sales analysis
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Country sales analysis DataFrame
        """
        logger.info("🌍 Creating country sales analysis")
        
        country_sales = df.groupBy("Country") \
                         .agg(
                             spark_sum("TotalAmount").alias("total_revenue"),
                             countDistinct("InvoiceNo").alias("total_orders"),
                             countDistinct("CustomerID").alias("unique_customers"),
                             avg("TotalAmount").alias("avg_order_value"),
                             spark_sum("Quantity").alias("total_quantity_sold")
                         ) \
                         .withColumn("total_revenue", spark_round(col("total_revenue"), 2)) \
                         .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2)) \
                         .withColumn("revenue_per_customer", 
                                   spark_round(col("total_revenue") / col("unique_customers"), 2)) \
                         .orderBy(desc("total_revenue"))
        
        row_count = country_sales.count()
        logger.info(f"✅ Created country sales analysis for {row_count:,} countries")
        
        return country_sales
    
    def create_top_products_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create top products analysis by revenue and quantity
        
        Args:
            df: Silver DataFrame
            
        Returns:
            Top products analysis DataFrame
        """
        logger.info("🏆 Creating top products analysis")
        
        product_sales = df.groupBy("StockCode", "Description") \
                         .agg(
                             spark_sum("TotalAmount").alias("total_revenue"),
                             spark_sum("Quantity").alias("total_quantity_sold"),
                             countDistinct("InvoiceNo").alias("total_orders"),
                             countDistinct("CustomerID").alias("unique_customers"),
                             avg("UnitPrice").alias("avg_unit_price")
                         ) \
                         .withColumn("total_revenue", spark_round(col("total_revenue"), 2)) \
                         .withColumn("avg_unit_price", spark_round(col("avg_unit_price"), 2)) \
                         .withColumn("revenue_per_order", 
                                   spark_round(col("total_revenue") / col("total_orders"), 2))
        
        # Add rankings
        revenue_window = Window.orderBy(desc("total_revenue"))
        quantity_window = Window.orderBy(desc("total_quantity_sold"))
        
        product_sales = product_sales.withColumn("revenue_rank", rank().over(revenue_window)) \
                                   .withColumn("quantity_rank", rank().over(quantity_window)) \
                                   .orderBy("revenue_rank")
        
        row_count = product_sales.count()
        logger.info(f"✅ Created product analysis for {row_count:,} products")
        
        return product_sales
    
    def create_rfm_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create RFM (Recency, Frequency, Monetary) analysis for customer segmentation
        
        Args:
            df: Silver DataFrame with registered customers
            
        Returns:
            RFM analysis DataFrame
        """
        logger.info("🎯 Creating RFM customer analysis")
        
        # Filter out guest customers
        customer_df = df.filter(col("CustomerID").isNotNull())
        
        # Calculate analysis date (latest date in dataset)
        max_date = customer_df.select(spark_max("InvoiceTimestamp")).collect()[0][0]
        
        # Customer-level aggregations
        customer_metrics = customer_df.groupBy("CustomerID") \
                                    .agg(
                                        spark_max("InvoiceTimestamp").alias("last_purchase_date"),
                                        countDistinct("InvoiceNo").alias("frequency"),
                                        spark_sum("TotalAmount").alias("monetary_value")
                                    )
        
        # Calculate recency (days since last purchase)
        customer_metrics = customer_metrics.withColumn(
            "recency",
            datediff(lit(max_date), col("last_purchase_date"))
        )
        
        # Calculate RFM scores using percentiles
        rfm_percentiles = customer_metrics.select(
            percentile_approx("recency", 0.2).alias("recency_20"),
            percentile_approx("recency", 0.4).alias("recency_40"),
            percentile_approx("recency", 0.6).alias("recency_60"),
            percentile_approx("recency", 0.8).alias("recency_80"),
            percentile_approx("frequency", 0.2).alias("frequency_20"),
            percentile_approx("frequency", 0.4).alias("frequency_40"),
            percentile_approx("frequency", 0.6).alias("frequency_60"),
            percentile_approx("frequency", 0.8).alias("frequency_80"),
            percentile_approx("monetary_value", 0.2).alias("monetary_20"),
            percentile_approx("monetary_value", 0.4).alias("monetary_40"),
            percentile_approx("monetary_value", 0.6).alias("monetary_60"),
            percentile_approx("monetary_value", 0.8).alias("monetary_80")
        ).collect()[0]
        
        # Create RFM scores (1-5 scale, 5 being best)
        rfm_scores = customer_metrics.withColumn(
            "recency_score",
            when(col("recency") <= rfm_percentiles["recency_20"], 5)
            .when(col("recency") <= rfm_percentiles["recency_40"], 4)
            .when(col("recency") <= rfm_percentiles["recency_60"], 3)
            .when(col("recency") <= rfm_percentiles["recency_80"], 2)
            .otherwise(1)
        ).withColumn(
            "frequency_score",
            when(col("frequency") >= rfm_percentiles["frequency_80"], 5)
            .when(col("frequency") >= rfm_percentiles["frequency_60"], 4)
            .when(col("frequency") >= rfm_percentiles["frequency_40"], 3)
            .when(col("frequency") >= rfm_percentiles["frequency_20"], 2)
            .otherwise(1)
        ).withColumn(
            "monetary_score",
            when(col("monetary_value") >= rfm_percentiles["monetary_80"], 5)
            .when(col("monetary_value") >= rfm_percentiles["monetary_60"], 4)
            .when(col("monetary_value") >= rfm_percentiles["monetary_40"], 3)
            .when(col("monetary_value") >= rfm_percentiles["monetary_20"], 2)
            .otherwise(1)
        )
        
        # Create customer segments based on RFM scores
        rfm_analysis = rfm_scores.withColumn(
            "customer_segment",
            when((col("recency_score") >= 4) & (col("frequency_score") >= 4) & (col("monetary_score") >= 4), "Champions")
            .when((col("recency_score") >= 3) & (col("frequency_score") >= 3) & (col("monetary_score") >= 3), "Loyal Customers")
            .when((col("recency_score") >= 4) & (col("frequency_score") <= 2), "New Customers")
            .when((col("recency_score") <= 2) & (col("frequency_score") >= 3), "At Risk")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") >= 3), "Can't Lose Them")
            .when((col("recency_score") <= 2) & (col("frequency_score") <= 2) & (col("monetary_score") <= 2), "Lost")
            .otherwise("Potential Loyalists")
        ).withColumn("monetary_value", spark_round(col("monetary_value"), 2))
        
        row_count = rfm_analysis.count()
        logger.info(f"✅ Created RFM analysis for {row_count:,} customers")
        
        return rfm_analysis
    
    def create_cltv_analysis(self, df: DataFrame) -> DataFrame:
        """
        Create Customer Lifetime Value (CLTV) analysis
        
        Args:
            df: Silver DataFrame
            
        Returns:
            CLTV analysis DataFrame
        """
        logger.info("💰 Creating CLTV analysis")
        
        # Filter registered customers
        customer_df = df.filter(col("CustomerID").isNotNull())
        
        # Calculate customer metrics
        customer_metrics = customer_df.groupBy("CustomerID") \
                                    .agg(
                                        spark_min("InvoiceTimestamp").alias("first_purchase"),
                                        spark_max("InvoiceTimestamp").alias("last_purchase"),
                                        countDistinct("InvoiceNo").alias("total_orders"),
                                        spark_sum("TotalAmount").alias("total_spent"),
                                        avg("TotalAmount").alias("avg_order_value"),
                                        count("InvoiceTimestamp").alias("total_transactions")
                                    )
        
        # Calculate customer lifespan in days
        cltv_metrics = customer_metrics.withColumn(
            "customer_lifespan_days",
            datediff(col("last_purchase"), col("first_purchase")) + 1
        ).withColumn(
            "purchase_frequency",
            col("total_orders") / col("customer_lifespan_days")
        ).withColumn(
            "predicted_cltv",
            spark_round(col("avg_order_value") * col("purchase_frequency") * lit(365), 2)
        ).withColumn("total_spent", spark_round(col("total_spent"), 2)) \
         .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
        
        # Add CLTV segments
        cltv_percentiles = cltv_metrics.select(
            percentile_approx("predicted_cltv", 0.8).alias("cltv_80"),
            percentile_approx("predicted_cltv", 0.6).alias("cltv_60"),
            percentile_approx("predicted_cltv", 0.4).alias("cltv_40"),
            percentile_approx("predicted_cltv", 0.2).alias("cltv_20")
        ).collect()[0]
        
        cltv_analysis = cltv_metrics.withColumn(
            "cltv_segment",
            when(col("predicted_cltv") >= cltv_percentiles["cltv_80"], "High Value")
            .when(col("predicted_cltv") >= cltv_percentiles["cltv_60"], "Medium-High Value")
            .when(col("predicted_cltv") >= cltv_percentiles["cltv_40"], "Medium Value")
            .when(col("predicted_cltv") >= cltv_percentiles["cltv_20"], "Low-Medium Value")
            .otherwise("Low Value")
        ).orderBy(desc("predicted_cltv"))
        
        row_count = cltv_analysis.count()
        logger.info(f"✅ Created CLTV analysis for {row_count:,} customers")
        
        return cltv_analysis
    
    def write_to_gold(self, df: DataFrame, table_name: str, mode: str = "overwrite") -> None:
        """
        Write analytics data to Gold layer
        
        Args:
            df: Analytics DataFrame to write
            table_name: Gold table name
            mode: Write mode (overwrite, append)
        """
        gold_path = f"{self.config.storage.gold_path}/{table_name}"
        full_path = self.config.storage.get_absolute_path(gold_path)
        
        logger.info(f"💾 Writing to Gold table: {table_name}")
        
        try:
            df.write.format("delta") \
              .mode(mode) \
              .option("overwriteSchema", "true") \
              .save(full_path)
            
            row_count = df.count()
            
            logger.info(
                f"✅ Successfully wrote {row_count:,} rows to Gold",
                table_name=table_name,
                path=full_path,
                mode=mode,
                row_count=row_count
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to write to Gold: {str(e)}", table_name=table_name)
            raise
    
    @log_execution_time(logger)
    def run_gold_analytics(self, silver_table: str = "online_retail_cleaned") -> None:
        """
        Complete Gold analytics pipeline
        
        Args:
            silver_table: Source Silver table name
        """
        pipeline_start = time.time()
        
        logger.log_pipeline_start(
            "gold_analytics",
            silver_table=silver_table
        )
        
        try:
            # Read from Silver
            df_silver = self.read_from_silver(silver_table)
            
            # Create daily sales summary
            daily_sales = self.create_daily_sales_summary(df_silver)
            self.write_to_gold(daily_sales, "daily_sales")
            
            # Create country sales analysis
            country_sales = self.create_country_sales_analysis(df_silver)
            self.write_to_gold(country_sales, "country_sales")
            
            # Create top products analysis
            top_products = self.create_top_products_analysis(df_silver)
            self.write_to_gold(top_products, "top_products")
            
            # Create RFM analysis
            rfm_analysis = self.create_rfm_analysis(df_silver)
            self.write_to_gold(rfm_analysis, "rfm_table")
            
            # Create CLTV analysis
            cltv_analysis = self.create_cltv_analysis(df_silver)
            self.write_to_gold(cltv_analysis, "cltv_table")
            
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "gold_analytics",
                duration=duration,
                status="success",
                silver_table=silver_table,
                tables_created=5
            )
            
        except Exception as e:
            duration = time.time() - pipeline_start
            
            logger.log_pipeline_end(
                "gold_analytics",
                duration=duration,
                status="failed",
                error=str(e)
            )
            raise
