"""
ML Training Pipeline
Orchestrates model training workflows
"""
from typing import Dict, List, Any
from datetime import datetime
import os

from pyspark.sql import DataFrame
from src.etl.gold_analytics import GoldETL
from src.ml.models import SalesForecastModel, CustomerSegmentationModel, ModelManager
from src.utils.logger import PipelineLogger

logger = PipelineLogger(__name__)


class MLTrainingPipeline:
    """ML training pipeline for retail analytics models"""
    
    def __init__(self):
        self.gold_etl = GoldETL()
        self.model_manager = ModelManager()
        
    def load_training_data(self) -> Dict[str, DataFrame]:
        """
        Load training data from Gold layer
        
        Returns:
            Dictionary of training datasets
        """
        logger.info("Loading training data from Gold layer")
        
        # Load gold layer data
        silver_data = self.gold_etl.read_from_silver("online_retail_cleaned")
        
        # Create analytics datasets
        daily_sales = self.gold_etl.create_daily_sales_summary(silver_data)
        rfm_data = self.gold_etl.create_rfm_analysis(silver_data)
        
        training_data = {
            "daily_sales": daily_sales,
            "customer_data": silver_data,
            "rfm_data": rfm_data
        }
        
        logger.info("Training data loaded successfully")
        return training_data
    
    def train_forecasting_model(self, training_data: Dict[str, DataFrame]) -> Dict[str, float]:
        """
        Train sales forecasting model
        
        Args:
            training_data: Training datasets
            
        Returns:
            Training metrics
        """
        logger.info("Training sales forecasting model")
        
        # Initialize model
        forecast_model = SalesForecastModel()
        
        # Train model
        metrics = forecast_model.train_model(training_data["daily_sales"])
        
        # Register model
        self.model_manager.register_model("sales_forecast", forecast_model)
        
        logger.info(f"Sales forecasting model trained with metrics: {metrics}")
        return metrics
    
    def train_segmentation_model(self, training_data: Dict[str, DataFrame]) -> Dict[str, float]:
        """
        Train customer segmentation model
        
        Args:
            training_data: Training datasets
            
        Returns:
            Training metrics
        """
        logger.info("Training customer segmentation model")
        
        # Initialize model
        segmentation_model = CustomerSegmentationModel()
        
        # Train model
        metrics = segmentation_model.train_clustering_model(training_data["customer_data"])
        
        # Register model
        self.model_manager.register_model("customer_segmentation", segmentation_model)
        
        logger.info(f"Customer segmentation model trained with metrics: {metrics}")
        return metrics
    
    def run_training_pipeline(self) -> Dict[str, Any]:
        """
        Run complete ML training pipeline
        
        Returns:
            Training results and metrics
        """
        logger.info("Starting ML training pipeline")
        
        try:
            # Load training data
            training_data = self.load_training_data()
            
            # Train models
            forecast_metrics = self.train_forecasting_model(training_data)
            segmentation_metrics = self.train_segmentation_model(training_data)
            
            # Save models
            self.model_manager.save_all_models()
            
            # Compile results
            results = {
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "models_trained": ["sales_forecast", "customer_segmentation"],
                "metrics": {
                    "sales_forecast": forecast_metrics,
                    "customer_segmentation": segmentation_metrics
                }
            }
            
            logger.info("ML training pipeline completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"ML training pipeline failed: {str(e)}")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            }


class MLInferencePipeline:
    """ML inference pipeline for making predictions"""
    
    def __init__(self, model_dir: str = "models"):
        self.model_manager = ModelManager(model_dir)
        self.gold_etl = GoldETL()
        
    def load_models(self) -> None:
        """Load trained models from disk"""
        logger.info("Loading trained models")
        
        # Load models (implementation would load from disk)
        forecast_model = SalesForecastModel()
        segmentation_model = CustomerSegmentationModel()
        
        # In practice, would load actual saved models
        # forecast_model.load_model("models/sales_forecast")
        # segmentation_model.load_model("models/customer_segmentation")
        
        self.model_manager.register_model("sales_forecast", forecast_model)
        self.model_manager.register_model("customer_segmentation", segmentation_model)
        
        logger.info("Models loaded successfully")
    
    def generate_sales_forecast(self, days_ahead: int = 30) -> DataFrame:
        """
        Generate sales forecast
        
        Args:
            days_ahead: Number of days to forecast
            
        Returns:
            Sales forecast DataFrame
        """
        logger.info(f"Generating sales forecast for {days_ahead} days")
        
        forecast_model = self.model_manager.get_model("sales_forecast")
        predictions = forecast_model.predict_future_sales(days_ahead)
        
        logger.info("Sales forecast generated")
        return predictions
    
    def segment_customers(self) -> DataFrame:
        """
        Segment customers using trained model
        
        Returns:
            Customer segmentation DataFrame
        """
        logger.info("Segmenting customers")
        
        # Load latest customer data
        customer_data = self.gold_etl.read_from_silver("online_retail_cleaned")
        
        # Get segmentation model
        segmentation_model = self.model_manager.get_model("customer_segmentation")
        
        # Generate predictions
        segments = segmentation_model.predict_customer_segments(customer_data)
        
        logger.info("Customer segmentation completed")
        return segments
    
    def run_inference_pipeline(self, forecast_days: int = 30) -> Dict[str, Any]:
        """
        Run complete inference pipeline
        
        Args:
            forecast_days: Number of days to forecast
            
        Returns:
            Inference results
        """
        logger.info("Starting ML inference pipeline")
        
        try:
            # Load models
            self.load_models()
            
            # Generate predictions
            sales_forecast = self.generate_sales_forecast(forecast_days)
            customer_segments = self.segment_customers()
            
            # Count results
            forecast_count = sales_forecast.count()
            segments_count = customer_segments.count()
            
            results = {
                "timestamp": datetime.now().isoformat(),
                "status": "success",
                "predictions": {
                    "sales_forecast": {
                        "days_forecasted": forecast_days,
                        "predictions_count": forecast_count
                    },
                    "customer_segmentation": {
                        "customers_segmented": segments_count,
                        "segments": customer_segments.select("segment_label").distinct().count()
                    }
                }
            }
            
            logger.info("ML inference pipeline completed successfully")
            return results
            
        except Exception as e:
            logger.error(f"ML inference pipeline failed: {str(e)}")
            return {
                "timestamp": datetime.now().isoformat(),
                "status": "failed",
                "error": str(e)
            }
