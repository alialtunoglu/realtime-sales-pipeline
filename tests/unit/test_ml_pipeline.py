"""
Unit tests for ML Pipeline components
"""
import pytest
from unittest.mock import Mock, patch
from datetime import datetime

from src.ml.pipeline import MLTrainingPipeline, MLInferencePipeline
from src.ml.models import ModelManager
from src.ml.evaluation import ModelEvaluator, ModelMonitor


class TestModelManager:
    """Test cases for Model Manager"""
    
    @pytest.fixture
    def model_manager(self):
        """Create ModelManager instance"""
        return ModelManager("test_models")
    
    def test_register_and_get_model(self, model_manager):
        """Test model registration and retrieval"""
        mock_model = Mock()
        
        # Register model
        model_manager.register_model("test_model", mock_model)
        
        # Retrieve model
        retrieved_model = model_manager.get_model("test_model")
        assert retrieved_model is mock_model
    
    def test_get_nonexistent_model(self, model_manager):
        """Test getting non-existent model raises error"""
        with pytest.raises(ValueError, match="Model test_model not found"):
            model_manager.get_model("test_model")


class TestMLTrainingPipeline:
    """Test cases for ML Training Pipeline"""
    
    @pytest.fixture
    def training_pipeline(self):
        """Create MLTrainingPipeline instance"""
        pipeline = MLTrainingPipeline()
        pipeline.gold_etl = Mock()
        pipeline.model_manager = Mock()
        return pipeline
    
    def test_load_training_data(self, training_pipeline):
        """Test training data loading"""
        # Mock the ETL operations
        mock_silver_data = Mock()
        mock_daily_sales = Mock()
        mock_rfm_data = Mock()
        
        training_pipeline.gold_etl.read_from_silver.return_value = mock_silver_data
        training_pipeline.gold_etl.create_daily_sales_summary.return_value = mock_daily_sales
        training_pipeline.gold_etl.create_rfm_analysis.return_value = mock_rfm_data
        
        # Load training data
        training_data = training_pipeline.load_training_data()
        
        # Verify structure
        assert "daily_sales" in training_data
        assert "customer_data" in training_data
        assert "rfm_data" in training_data
        
        # Verify ETL calls
        training_pipeline.gold_etl.read_from_silver.assert_called_once_with("online_retail_cleaned")
        training_pipeline.gold_etl.create_daily_sales_summary.assert_called_once()
        training_pipeline.gold_etl.create_rfm_analysis.assert_called_once()
    
    @patch('src.ml.pipeline.SalesForecastModel')
    def test_train_forecasting_model(self, mock_forecast_class, training_pipeline):
        """Test forecasting model training"""
        mock_model = Mock()
        mock_forecast_class.return_value = mock_model
        mock_model.train_model.return_value = {"mae": 100.0, "rmse": 120.0}
        
        training_data = {"daily_sales": Mock()}
        
        # Train model
        metrics = training_pipeline.train_forecasting_model(training_data)
        
        # Verify training
        mock_model.train_model.assert_called_once()
        training_pipeline.model_manager.register_model.assert_called_once_with("sales_forecast", mock_model)
        assert "mae" in metrics
    
    @patch('src.ml.pipeline.CustomerSegmentationModel')  
    def test_train_segmentation_model(self, mock_segmentation_class, training_pipeline):
        """Test segmentation model training"""
        mock_model = Mock()
        mock_segmentation_class.return_value = mock_model
        mock_model.train_clustering_model.return_value = {"silhouette_score": 0.75}
        
        training_data = {"customer_data": Mock()}
        
        # Train model
        metrics = training_pipeline.train_segmentation_model(training_data)
        
        # Verify training
        mock_model.train_clustering_model.assert_called_once()
        training_pipeline.model_manager.register_model.assert_called_once_with("customer_segmentation", mock_model)
        assert "silhouette_score" in metrics


class TestModelEvaluator:
    """Test cases for Model Evaluator"""
    
    @pytest.fixture
    def model_evaluator(self):
        """Create ModelEvaluator instance"""
        return ModelEvaluator()
    
    def test_business_impact_calculation(self, model_evaluator):
        """Test business impact calculation"""
        baseline_metrics = {"mae": 120.0, "r_squared": 0.65}
        current_metrics = {"mae": 100.0, "r_squared": 0.75}
        
        impact = model_evaluator.calculate_business_impact(baseline_metrics, current_metrics)
        
        # Check improvement calculations
        assert "mae_improvement_pct" in impact
        assert "r_squared_improvement_pct" in impact
        assert "forecast_accuracy_improvement" in impact
        assert "model_reliability_score" in impact


class TestModelMonitor:
    """Test cases for Model Monitor"""
    
    @pytest.fixture
    def model_monitor(self):
        """Create ModelMonitor instance"""
        return ModelMonitor()
    
    def test_healthy_model_check(self, model_monitor):
        """Test health check for healthy model"""
        metrics = {"r_squared": 0.85, "mae": 50.0, "mape": 8.5}
        
        health_report = model_monitor.check_model_health(metrics)
        
        assert health_report["status"] == "healthy"
        assert len(health_report["alerts"]) == 0
        assert len(health_report["passed_checks"]) > 0
    
    def test_unhealthy_model_check(self, model_monitor):
        """Test health check for unhealthy model"""
        metrics = {"r_squared": 0.45, "mae": 1500.0, "mape": 25.0}
        
        health_report = model_monitor.check_model_health(metrics)
        
        assert health_report["status"] in ["warning", "critical"]
        assert len(health_report["alerts"]) > 0
    
    def test_performance_report_generation(self, model_monitor):
        """Test performance report generation"""
        evaluation_results = {
            "sales_forecast": {"mae": 100.0, "r_squared": 0.8},
            "customer_segmentation": {"balance_ratio": 0.6}
        }
        
        report = model_monitor.generate_performance_report(evaluation_results)
        
        assert "report_timestamp" in report
        assert "model_performance" in report
        assert "health_checks" in report
        assert "summary" in report
        assert report["summary"]["models_evaluated"] == 2
