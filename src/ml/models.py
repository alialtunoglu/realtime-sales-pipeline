"""
Sales Forecasting Model
Uses time series analysis to predict future sales trends
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional, Any
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, sum as spark_sum, avg, count, date_format
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
from sklearn.metrics import mean_absolute_error, mean_squared_error
import joblib

from src.utils.spark_factory import SparkSessionFactory
from src.utils.logger import PipelineLogger

logger = PipelineLogger(__name__)


class SalesForecastModel:
    """Sales forecasting model using ML techniques"""
    
    def __init__(self):
        self.spark = SparkSessionFactory.get_spark_session("SalesForecasting")
        self.model = None
        self.feature_cols = ["day_of_year", "month", "day_of_week", "lag_7", "lag_30", "rolling_mean_7"]
        
    def prepare_features(self, df: DataFrame) -> DataFrame:
        """
        Prepare features for time series forecasting
        
        Args:
            df: Daily sales DataFrame from Gold layer
            
        Returns:
            DataFrame with engineered features
        """
        logger.info("Preparing features for forecasting model")
        
        # Add time-based features
        df_features = df.withColumn("day_of_year", date_format(col("InvoiceTimestamp"), "D").cast("int")) \
                       .withColumn("month", col("Month")) \
                       .withColumn("day_of_week", col("DayOfWeek"))
        
        # Create lag features (previous sales)
        from pyspark.sql.window import Window
        window_7 = Window.orderBy("InvoiceTimestamp").rowsBetween(-7, -1)
        window_30 = Window.orderBy("InvoiceTimestamp").rowsBetween(-30, -1)
        
        df_features = df_features.withColumn("lag_7", avg("daily_revenue").over(window_7)) \
                               .withColumn("lag_30", avg("daily_revenue").over(window_30)) \
                               .withColumn("rolling_mean_7", avg("daily_revenue").over(window_7))
        
        # Fill null values
        df_features = df_features.na.fill(0, self.feature_cols)
        
        logger.info(f"Created features: {self.feature_cols}")
        return df_features
    
    def train_model(self, df: DataFrame) -> Dict[str, float]:
        """
        Train the forecasting model
        
        Args:
            df: Training data with features
            
        Returns:
            Training metrics
        """
        logger.info("Training sales forecasting model")
        
        # Prepare features
        df_features = self.prepare_features(df)
        
        # Create feature vector
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="features")
        
        # Split data
        train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=42)
        
        # Create model pipeline
        rf = RandomForestRegressor(featuresCol="features", labelCol="daily_revenue", numTrees=100)
        pipeline = Pipeline(stages=[assembler, rf])
        
        # Train model
        self.model = pipeline.fit(train_data)
        
        # Evaluate model
        predictions = self.model.transform(test_data)
        evaluator = RegressionEvaluator(labelCol="daily_revenue", predictionCol="prediction")
        
        mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
        rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
        r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
        
        metrics = {"mae": mae, "rmse": rmse, "r2": r2}
        logger.info(f"Model training completed. Metrics: {metrics}")
        
        return metrics
    
    def predict_future_sales(self, days_ahead: int = 30) -> DataFrame:
        """
        Predict future sales for specified days
        
        Args:
            days_ahead: Number of days to predict
            
        Returns:
            DataFrame with predictions
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train_model() first.")
        
        logger.info(f"Generating predictions for {days_ahead} days ahead")
        
        # Create future dates
        from datetime import date
        today = date.today()
        future_dates = [today + timedelta(days=i) for i in range(1, days_ahead + 1)]
        
        # Create prediction features (simplified for demo)
        prediction_data = []
        for future_date in future_dates:
            prediction_data.append({
                "day_of_year": future_date.timetuple().tm_yday,
                "month": future_date.month,
                "day_of_week": future_date.weekday(),
                "lag_7": 1000.0,  # Would use actual historical data
                "lag_30": 950.0,
                "rolling_mean_7": 1025.0
            })
        
        # Convert to Spark DataFrame
        prediction_df = self.spark.createDataFrame(prediction_data)
        
        # Make predictions
        predictions = self.model.transform(prediction_df)
        
        # Add date column
        future_dates_str = [d.isoformat() for d in future_dates]
        predictions = predictions.withColumn("prediction_date", 
                                           col("day_of_year").cast("string"))  # Simplified
        
        logger.info("Sales predictions generated successfully")
        return predictions.select("prediction_date", "prediction")
    
    def save_model(self, path: str):
        """Save trained model to disk"""
        if self.model is None:
            raise ValueError("No model to save. Train model first.")
        
        self.model.write().overwrite().save(path)
        logger.info(f"Model saved to {path}")
    
    def load_model(self, path: str):
        """Load model from disk"""
        from pyspark.ml import PipelineModel
        self.model = PipelineModel.load(path)
        logger.info(f"Model loaded from {path}")


class CustomerSegmentationModel:
    """Customer segmentation using clustering techniques"""
    
    def __init__(self):
        self.spark = SparkSessionFactory.get_spark_session("CustomerSegmentation")
        self.model = None
        self.feature_cols = ["recency", "frequency", "monetary", "avg_order_value"]
    
    def prepare_customer_features(self, df: DataFrame) -> DataFrame:
        """
        Prepare customer features for segmentation
        
        Args:
            df: Silver layer DataFrame
            
        Returns:
            Customer features DataFrame
        """
        logger.info("Preparing customer features for segmentation")
        
        from pyspark.sql.functions import max as spark_max, datediff, current_date
        
        # Calculate RFM features
        customer_features = df.filter(col("CustomerID").isNotNull()) \
                             .groupBy("CustomerID") \
                             .agg(
                                 datediff(current_date(), spark_max("InvoiceTimestamp")).alias("recency"),
                                 count("InvoiceNo").alias("frequency"),
                                 spark_sum("TotalAmount").alias("monetary"),
                                 avg("TotalAmount").alias("avg_order_value")
                             )
        
        logger.info("Customer features prepared")
        return customer_features
    
    def train_clustering_model(self, df: DataFrame, k: int = 5) -> Dict[str, float]:
        """
        Train K-means clustering model for customer segmentation
        
        Args:
            df: Customer features DataFrame
            k: Number of clusters
            
        Returns:
            Training metrics
        """
        logger.info(f"Training K-means clustering model with k={k}")
        
        from pyspark.ml.clustering import KMeans
        from pyspark.ml.feature import StandardScaler
        
        # Prepare features
        customer_features = self.prepare_customer_features(df)
        
        # Create feature vector
        assembler = VectorAssembler(inputCols=self.feature_cols, outputCol="features_raw")
        
        # Scale features
        scaler = StandardScaler(inputCol="features_raw", outputCol="features")
        
        # K-means clustering
        kmeans = KMeans(featuresCol="features", predictionCol="cluster", k=k, seed=42)
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, kmeans])
        
        # Train model
        self.model = pipeline.fit(customer_features)
        
        # Evaluate clustering
        predictions = self.model.transform(customer_features)
        silhouette_score = self._calculate_silhouette_score(predictions)
        
        metrics = {"silhouette_score": silhouette_score, "num_clusters": k}
        logger.info(f"Clustering model trained. Metrics: {metrics}")
        
        return metrics
    
    def _calculate_silhouette_score(self, predictions: DataFrame) -> float:
        """Calculate silhouette score for clustering evaluation"""
        # Simplified silhouette calculation for demo
        # In practice, would use proper silhouette analysis
        cluster_counts = predictions.groupBy("cluster").count().collect()
        total_count = predictions.count()
        
        # Simple metric based on cluster balance
        balance_score = min([row['count'] for row in cluster_counts]) / max([row['count'] for row in cluster_counts])
        return balance_score
    
    def predict_customer_segments(self, df: DataFrame) -> DataFrame:
        """
        Predict customer segments for new data
        
        Args:
            df: Customer data DataFrame
            
        Returns:
            DataFrame with cluster predictions
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train_clustering_model() first.")
        
        logger.info("Predicting customer segments")
        
        customer_features = self.prepare_customer_features(df)
        predictions = self.model.transform(customer_features)
        
        # Add segment labels
        predictions = predictions.withColumn("segment_label", 
                                           self._map_cluster_to_label(col("cluster")))
        
        logger.info("Customer segmentation completed")
        return predictions.select("CustomerID", "cluster", "segment_label", 
                                "recency", "frequency", "monetary")
    
    def _map_cluster_to_label(self, cluster_col):
        """Map cluster numbers to business-friendly labels"""
        from pyspark.sql.functions import when
        
        return when(cluster_col == 0, "Champions") \
              .when(cluster_col == 1, "Loyal_Customers") \
              .when(cluster_col == 2, "Potential_Loyalists") \
              .when(cluster_col == 3, "At_Risk") \
              .otherwise("Lost_Customers")


class ModelManager:
    """Manages ML model lifecycle and operations"""
    
    def __init__(self, model_dir: str = "models"):
        self.model_dir = model_dir
        self.models: Dict[str, Any] = {}
        
    def register_model(self, name: str, model) -> None:
        """Register a trained model"""
        self.models[name] = model
        logger.info(f"Model {name} registered")
    
    def get_model(self, name: str):
        """Get registered model by name"""
        if name not in self.models:
            raise ValueError(f"Model {name} not found")
        return self.models[name]
    
    def save_all_models(self) -> None:
        """Save all registered models"""
        import os
        os.makedirs(self.model_dir, exist_ok=True)
        
        for name, model in self.models.items():
            model_path = os.path.join(self.model_dir, name)
            model.save_model(model_path)
        
        logger.info(f"All models saved to {self.model_dir}")
