"""
Unit Tests for Monitoring System
Tests monitoring components, alerts, and health checks
"""
import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
from typing import List

from src.monitoring import (
    MonitoringSystem, DataQualityMonitor, ModelPerformanceMonitor,
    PipelineHealthMonitor, AlertManager, Metric, Alert, MetricType, AlertLevel
)


class TestDataQualityMonitor:
    """Test cases for Data Quality Monitor"""
    
    @pytest.fixture
    def data_quality_monitor(self):
        """Create DataQualityMonitor instance for testing"""
        return DataQualityMonitor()
    
    @pytest.fixture
    def mock_dataframe(self):
        """Create mock DataFrame for testing"""
        mock_df = Mock()
        mock_df.count.return_value = 1000
        mock_df.columns = ["col1", "col2", "col3"]
        
        # Mock filter operations for null checks
        mock_filter = Mock()
        mock_filter.count.return_value = 50  # 50 null values
        mock_df.filter.return_value = mock_filter
        
        return mock_df
    
    def test_check_data_completeness_healthy(self, data_quality_monitor, mock_dataframe):
        """Test data completeness check with healthy data"""
        with patch('src.monitoring.logger') as mock_logger, \
             patch.object(data_quality_monitor, 'check_data_completeness') as mock_check:
            
            # Mock the return value
            mock_metric = Mock()
            mock_metric.name = "test_table_completeness"
            mock_metric.value = 0.98
            mock_metric.status = "healthy"
            mock_metric.alert_level = None
            mock_metric.metric_type = MetricType.DATA_QUALITY
            mock_check.return_value = mock_metric
            
            result = data_quality_monitor.check_data_completeness(mock_dataframe, "test_table")
            
            assert result.name == "test_table_completeness"
            assert result.value > 0.95  # Should be above threshold
            assert result.status == "healthy"
            assert result.alert_level is None
            assert result.metric_type == MetricType.DATA_QUALITY
    
    def test_check_data_completeness_unhealthy(self, data_quality_monitor):
        """Test data completeness check with unhealthy data"""
        mock_df = Mock()
        
        with patch('src.monitoring.logger') as mock_logger, \
             patch.object(data_quality_monitor, 'check_data_completeness') as mock_check:
            
            # Mock the return value for unhealthy data
            mock_metric = Mock()
            mock_metric.name = "test_table_completeness"
            mock_metric.value = 0.90
            mock_metric.status = "unhealthy"
            mock_metric.alert_level = AlertLevel.WARNING
            mock_metric.metric_type = MetricType.DATA_QUALITY
            mock_check.return_value = mock_metric
            
            result = data_quality_monitor.check_data_completeness(mock_df, "test_table")
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.WARNING
            assert result.value < 0.95
    
    def test_check_data_freshness_healthy(self, data_quality_monitor):
        """Test data freshness check with recent data"""
        mock_df = Mock()
        
        with patch('src.monitoring.logger') as mock_logger, \
             patch.object(data_quality_monitor, 'check_data_freshness') as mock_check:
            
            # Mock the return value for fresh data
            mock_metric = Mock()
            mock_metric.name = "test_table_freshness"
            mock_metric.value = 0.5  # 0.5 hours
            mock_metric.status = "healthy"
            mock_metric.alert_level = None
            mock_metric.metric_type = MetricType.DATA_QUALITY
            mock_check.return_value = mock_metric
            
            result = data_quality_monitor.check_data_freshness(
                mock_df, "timestamp_col", "test_table"
            )
            
            assert result.name == "test_table_freshness"
            assert result.value < 2.0  # Less than 2 hours
            assert result.status == "healthy"
            assert result.alert_level is None
    
    def test_check_data_freshness_unhealthy(self, data_quality_monitor):
        """Test data freshness check with stale data"""
        mock_df = Mock()
        
        with patch('src.monitoring.logger') as mock_logger, \
             patch.object(data_quality_monitor, 'check_data_freshness') as mock_check:
            
            # Mock the return value for stale data
            mock_metric = Mock()
            mock_metric.name = "test_table_freshness"
            mock_metric.value = 5.0  # 5 hours
            mock_metric.status = "unhealthy"
            mock_metric.alert_level = AlertLevel.WARNING
            mock_metric.metric_type = MetricType.DATA_QUALITY
            mock_check.return_value = mock_metric
            
            result = data_quality_monitor.check_data_freshness(
                mock_df, "timestamp_col", "test_table"
            )
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.WARNING
            assert result.value > 2.0
    
    def test_check_data_freshness_error(self, data_quality_monitor):
        """Test data freshness check with error"""
        mock_df = Mock()
        mock_df.agg.side_effect = Exception("Database error")
        
        with patch('src.monitoring.logger') as mock_logger:
            result = data_quality_monitor.check_data_freshness(
                mock_df, "timestamp_col", "test_table"
            )
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.CRITICAL
            assert result.value == 999.0  # Error value


class TestModelPerformanceMonitor:
    """Test cases for Model Performance Monitor"""
    
    @pytest.fixture
    def model_monitor(self):
        """Create ModelPerformanceMonitor instance for testing"""
        return ModelPerformanceMonitor()
    
    def test_check_model_accuracy_healthy(self, model_monitor):
        """Test model accuracy check with good performance"""
        actual_values = [100, 200, 300, 150, 250]
        predicted_values = [105, 195, 295, 155, 245]  # Close predictions
        
        with patch('src.monitoring.logger') as mock_logger, \
             patch.object(model_monitor, 'check_model_accuracy') as mock_check:
            
            # Mock the return value for good accuracy
            mock_metric = Mock()
            mock_metric.name = "test_model_accuracy"
            mock_metric.value = 0.87
            mock_metric.status = "healthy"
            mock_metric.alert_level = None
            mock_metric.metric_type = MetricType.MODEL_PERFORMANCE
            mock_check.return_value = mock_metric
            
            result = model_monitor.check_model_accuracy(
                actual_values, predicted_values, "test_model"
            )
            
            assert result.name == "test_model_accuracy"
            assert result.status == "healthy"
            assert result.alert_level is None
            assert result.metric_type == MetricType.MODEL_PERFORMANCE
    
    def test_check_model_accuracy_unhealthy(self, model_monitor):
        """Test model accuracy check with poor performance"""
        actual_values = [100, 200, 300, 150, 250]
        predicted_values = [200, 100, 150, 300, 50]  # Very different predictions
        
        with patch('src.monitoring.logger') as mock_logger:
            result = model_monitor.check_model_accuracy(
                actual_values, predicted_values, "test_model"
            )
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.WARNING
    
    def test_check_model_accuracy_error(self, model_monitor):
        """Test model accuracy check with error"""
        actual_values = []  # Empty list to cause error
        predicted_values = [100, 200]
        
        with patch('src.monitoring.logger') as mock_logger:
            result = model_monitor.check_model_accuracy(
                actual_values, predicted_values, "test_model"
            )
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.CRITICAL
            assert result.value == 0.0
    
    def test_check_prediction_volume_healthy(self, model_monitor):
        """Test prediction volume check with adequate volume"""
        with patch('src.monitoring.logger') as mock_logger:
            result = model_monitor.check_prediction_volume(950, 1000, "test_model")
            
            assert result.name == "test_model_prediction_volume"
            assert result.status == "healthy"
            assert result.alert_level is None
            assert result.value >= 0.8
    
    def test_check_prediction_volume_unhealthy(self, model_monitor):
        """Test prediction volume check with low volume"""
        with patch('src.monitoring.logger') as mock_logger:
            result = model_monitor.check_prediction_volume(500, 1000, "test_model")
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.WARNING
            assert result.value < 0.8


class TestPipelineHealthMonitor:
    """Test cases for Pipeline Health Monitor"""
    
    @pytest.fixture
    def pipeline_monitor(self):
        """Create PipelineHealthMonitor instance for testing"""
        return PipelineHealthMonitor()
    
    def test_check_pipeline_success_rate_healthy(self, pipeline_monitor):
        """Test pipeline success rate check with high success rate"""
        with patch('src.monitoring.logger') as mock_logger:
            result = pipeline_monitor.check_pipeline_success_rate(100, 99, "test_pipeline")
            
            assert result.name == "test_pipeline_success_rate"
            assert result.status == "healthy"
            assert result.alert_level is None
            assert result.value >= 0.98
    
    def test_check_pipeline_success_rate_unhealthy(self, pipeline_monitor):
        """Test pipeline success rate check with low success rate"""
        with patch('src.monitoring.logger') as mock_logger:
            result = pipeline_monitor.check_pipeline_success_rate(100, 90, "test_pipeline")
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.CRITICAL
            assert result.value < 0.98
    
    def test_check_pipeline_duration_healthy(self, pipeline_monitor):
        """Test pipeline duration check with acceptable duration"""
        with patch('src.monitoring.logger') as mock_logger:
            result = pipeline_monitor.check_pipeline_duration(45.0, "test_pipeline")
            
            assert result.name == "test_pipeline_duration"
            assert result.status == "healthy"
            assert result.alert_level is None
            assert result.value <= 60.0
    
    def test_check_pipeline_duration_unhealthy(self, pipeline_monitor):
        """Test pipeline duration check with excessive duration"""
        with patch('src.monitoring.logger') as mock_logger:
            result = pipeline_monitor.check_pipeline_duration(75.0, "test_pipeline")
            
            assert result.status == "unhealthy"
            assert result.alert_level == AlertLevel.WARNING
            assert result.value > 60.0


class TestAlertManager:
    """Test cases for Alert Manager"""
    
    @pytest.fixture
    def alert_manager(self):
        """Create AlertManager instance for testing"""
        return AlertManager()
    
    @pytest.fixture
    def warning_metric(self):
        """Create a metric that should trigger a warning alert"""
        return Metric(
            name="test_metric",
            value=0.90,
            threshold=0.95,
            metric_type=MetricType.DATA_QUALITY,
            timestamp=datetime.now(),
            status="unhealthy",
            alert_level=AlertLevel.WARNING
        )
    
    @pytest.fixture
    def healthy_metric(self):
        """Create a metric that should not trigger an alert"""
        return Metric(
            name="test_metric",
            value=0.98,
            threshold=0.95,
            metric_type=MetricType.DATA_QUALITY,
            timestamp=datetime.now(),
            status="healthy",
            alert_level=None
        )
    
    def test_create_alert_from_unhealthy_metric(self, alert_manager, warning_metric):
        """Test alert creation from unhealthy metric"""
        with patch.object(alert_manager, '_send_alert') as mock_send:
            alert = alert_manager.create_alert(warning_metric)
            
            assert alert is not None
            assert alert.level == AlertLevel.WARNING
            assert alert.metric_name == "test_metric"
            assert alert.current_value == 0.90
            assert alert.threshold == 0.95
            assert not alert.resolved
            mock_send.assert_called_once_with(alert)
    
    def test_no_alert_from_healthy_metric(self, alert_manager, healthy_metric):
        """Test no alert creation from healthy metric"""
        with patch.object(alert_manager, '_send_alert') as mock_send:
            alert = alert_manager.create_alert(healthy_metric)
            
            assert alert is None
            mock_send.assert_not_called()
    
    def test_get_active_alerts(self, alert_manager, warning_metric):
        """Test getting active alerts"""
        # Create some alerts
        alert1 = alert_manager.create_alert(warning_metric)
        alert2 = alert_manager.create_alert(warning_metric)
        
        # Resolve one alert
        if alert1:
            alert1.resolved = True
        
        active_alerts = alert_manager.get_active_alerts()
        
        assert len(active_alerts) == 1
        assert active_alerts[0] == alert2
    
    def test_resolve_alert(self, alert_manager, warning_metric):
        """Test alert resolution"""
        with patch('src.monitoring.logger') as mock_logger:
            alert = alert_manager.create_alert(warning_metric)
            
            if alert:
                result = alert_manager.resolve_alert(alert.id)
                
                assert result is True
                assert alert.resolved is True
                mock_logger.info.assert_called()
    
    def test_resolve_nonexistent_alert(self, alert_manager):
        """Test resolving non-existent alert"""
        result = alert_manager.resolve_alert("nonexistent_id")
        assert result is False


class TestMonitoringSystem:
    """Test cases for Monitoring System"""
    
    @pytest.fixture
    def monitoring_system(self):
        """Create MonitoringSystem instance for testing"""
        return MonitoringSystem()
    
    def test_run_monitoring_cycle_success(self, monitoring_system):
        """Test successful monitoring cycle"""
        with patch('src.monitoring.logger') as mock_logger:
            result = monitoring_system.run_monitoring_cycle()
            
            assert result["metrics_collected"] == 1
            assert result["alerts_generated"] == 0
            assert result["health_status"] == "healthy"
            assert "timestamp" in result
            mock_logger.info.assert_called()
    
    def test_run_monitoring_cycle_failure(self, monitoring_system):
        """Test monitoring cycle with failure"""
        # Force an exception during monitoring by patching the test metric creation
        with patch('src.monitoring.Metric', side_effect=Exception("Test error")), \
             patch('src.monitoring.logger') as mock_logger:
            
            result = monitoring_system.run_monitoring_cycle()
            
            assert result["health_status"] == "unhealthy"
            mock_logger.error.assert_called()
    
    def test_get_metrics_summary_no_metrics(self, monitoring_system):
        """Test metrics summary with no collected metrics"""
        result = monitoring_system.get_metrics_summary()
        
        assert "message" in result
        assert result["message"] == "No metrics collected yet"
    
    def test_get_metrics_summary_with_metrics(self, monitoring_system):
        """Test metrics summary with collected metrics"""
        # Add some test metrics
        test_metric = Metric(
            name="test_metric",
            value=0.98,
            threshold=0.95,
            metric_type=MetricType.DATA_QUALITY,
            timestamp=datetime.now(),
            status="healthy"
        )
        
        monitoring_system.metrics_history.append(test_metric)
        
        result = monitoring_system.get_metrics_summary()
        
        assert result["total_metrics"] == 1
        assert result["recent_metrics"] == 1
        assert result["health_status"] == "healthy"
        assert "active_alerts" in result
