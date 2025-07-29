"""
Production Monitoring System
Handles model performance, data quality, and pipeline health monitoring
"""
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from enum import Enum
import json

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, isnan, isnull, when, sum as spark_sum

from config.settings import config
from src.utils.logger import logger


class AlertLevel(Enum):
    """Alert severity levels"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics to monitor"""
    DATA_QUALITY = "data_quality"
    MODEL_PERFORMANCE = "model_performance"
    PIPELINE_HEALTH = "pipeline_health"
    BUSINESS_KPI = "business_kpi"


@dataclass
class Metric:
    """Metric data structure"""
    name: str
    value: float
    threshold: float
    metric_type: MetricType
    timestamp: datetime
    status: str = "healthy"
    alert_level: Optional[AlertLevel] = None


@dataclass
class Alert:
    """Alert data structure"""
    id: str
    title: str
    description: str
    level: AlertLevel
    metric_name: str
    current_value: float
    threshold: float
    timestamp: datetime
    resolved: bool = False


class DataQualityMonitor:
    """Monitor data quality across all layers"""
    
    def __init__(self):
        self.thresholds = {
            "completeness": 0.95,  # 95% data completeness required
            "accuracy": 0.98,      # 98% accuracy required
            "consistency": 0.99,   # 99% consistency required
            "freshness_hours": 2   # Data should be < 2 hours old
        }
    
    def check_data_completeness(self, df: DataFrame, table_name: str) -> Metric:
        """Check data completeness"""
        total_rows = df.count()
        total_cells = total_rows * len(df.columns)
        
        # Count null values across all columns
        null_counts = []
        for column in df.columns:
            null_count = df.filter(col(column).isNull() | isnan(col(column))).count()
            null_counts.append(null_count)
        
        total_nulls = sum(null_counts)
        completeness = (total_cells - total_nulls) / total_cells if total_cells > 0 else 0
        
        status = "healthy" if completeness >= self.thresholds["completeness"] else "unhealthy"
        alert_level = None if status == "healthy" else AlertLevel.WARNING
        
        logger.info(
            f"📊 Data completeness check for {table_name}: {completeness:.3f}",
            table_name=table_name,
            completeness=completeness,
            total_rows=total_rows,
            total_nulls=total_nulls
        )
        
        return Metric(
            name=f"{table_name}_completeness",
            value=completeness,
            threshold=self.thresholds["completeness"],
            metric_type=MetricType.DATA_QUALITY,
            timestamp=datetime.now(),
            status=status,
            alert_level=alert_level
        )
    
    def check_data_freshness(self, df: DataFrame, timestamp_column: str, table_name: str) -> Metric:
        """Check data freshness"""
        try:
            latest_timestamp = df.agg({timestamp_column: "max"}).collect()[0][0]
            hours_old = (datetime.now() - latest_timestamp).total_seconds() / 3600
            
            status = "healthy" if hours_old <= self.thresholds["freshness_hours"] else "unhealthy"
            alert_level = None if status == "healthy" else AlertLevel.WARNING
            
            logger.info(
                f"⏰ Data freshness check for {table_name}: {hours_old:.2f} hours old",
                table_name=table_name,
                hours_old=hours_old,
                latest_timestamp=latest_timestamp
            )
            
            return Metric(
                name=f"{table_name}_freshness",
                value=hours_old,
                threshold=self.thresholds["freshness_hours"],
                metric_type=MetricType.DATA_QUALITY,
                timestamp=datetime.now(),
                status=status,
                alert_level=alert_level
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to check data freshness for {table_name}: {str(e)}")
            return Metric(
                name=f"{table_name}_freshness",
                value=999.0,  # Very high value to trigger alert
                threshold=self.thresholds["freshness_hours"],
                metric_type=MetricType.DATA_QUALITY,
                timestamp=datetime.now(),
                status="unhealthy",
                alert_level=AlertLevel.CRITICAL
            )


class ModelPerformanceMonitor:
    """Monitor ML model performance and drift"""
    
    def __init__(self):
        self.thresholds = {
            "accuracy_drift": 0.05,      # 5% accuracy drift threshold
            "prediction_volume": 0.8,    # 80% of expected volume
            "feature_drift": 0.1,        # 10% feature drift threshold
            "model_age_days": 30         # Model should be retrained after 30 days
        }
    
    def check_model_accuracy(self, actual_values: List[float], 
                           predicted_values: List[float], model_name: str) -> Metric:
        """Check model prediction accuracy"""
        from sklearn.metrics import mean_absolute_percentage_error
        
        try:
            mape = mean_absolute_percentage_error(actual_values, predicted_values)
            accuracy = 1 - mape
            
            # Compare with baseline (assuming we have a baseline stored)
            baseline_accuracy = 0.85  # This would come from model registry
            accuracy_drift = abs(accuracy - baseline_accuracy)
            
            status = "healthy" if accuracy_drift <= self.thresholds["accuracy_drift"] else "unhealthy"
            alert_level = None if status == "healthy" else AlertLevel.WARNING
            
            logger.info(
                f"🎯 Model accuracy check for {model_name}: {accuracy:.3f} (drift: {accuracy_drift:.3f})",
                model_name=model_name,
                accuracy=accuracy,
                baseline_accuracy=baseline_accuracy,
                accuracy_drift=accuracy_drift
            )
            
            return Metric(
                name=f"{model_name}_accuracy",
                value=accuracy,
                threshold=baseline_accuracy - self.thresholds["accuracy_drift"],
                metric_type=MetricType.MODEL_PERFORMANCE,
                timestamp=datetime.now(),
                status=status,
                alert_level=alert_level
            )
            
        except Exception as e:
            logger.error(f"❌ Failed to check model accuracy for {model_name}: {str(e)}")
            return Metric(
                name=f"{model_name}_accuracy",
                value=0.0,
                threshold=0.8,
                metric_type=MetricType.MODEL_PERFORMANCE,
                timestamp=datetime.now(),
                status="unhealthy",
                alert_level=AlertLevel.CRITICAL
            )
    
    def check_prediction_volume(self, predictions_count: int, 
                              expected_count: int, model_name: str) -> Metric:
        """Check if prediction volume is within expected range"""
        volume_ratio = predictions_count / expected_count if expected_count > 0 else 0
        
        status = "healthy" if volume_ratio >= self.thresholds["prediction_volume"] else "unhealthy"
        alert_level = None if status == "healthy" else AlertLevel.WARNING
        
        logger.info(
            f"📈 Prediction volume check for {model_name}: {predictions_count}/{expected_count} ({volume_ratio:.2f})",
            model_name=model_name,
            predictions_count=predictions_count,
            expected_count=expected_count,
            volume_ratio=volume_ratio
        )
        
        return Metric(
            name=f"{model_name}_prediction_volume",
            value=volume_ratio,
            threshold=self.thresholds["prediction_volume"],
            metric_type=MetricType.MODEL_PERFORMANCE,
            timestamp=datetime.now(),
            status=status,
            alert_level=alert_level
        )


class PipelineHealthMonitor:
    """Monitor overall pipeline health and performance"""
    
    def __init__(self):
        self.thresholds = {
            "success_rate": 0.98,        # 98% success rate required
            "avg_duration_minutes": 60,   # Pipeline should complete in < 60 min
            "memory_usage_gb": 8.0,      # Memory usage threshold
            "cpu_usage_percent": 80.0    # CPU usage threshold
        }
    
    def check_pipeline_success_rate(self, total_runs: int, 
                                   successful_runs: int, pipeline_name: str) -> Metric:
        """Check pipeline success rate"""
        success_rate = successful_runs / total_runs if total_runs > 0 else 0
        
        status = "healthy" if success_rate >= self.thresholds["success_rate"] else "unhealthy"
        alert_level = None if status == "healthy" else AlertLevel.CRITICAL
        
        logger.info(
            f"✅ Pipeline success rate for {pipeline_name}: {success_rate:.3f} ({successful_runs}/{total_runs})",
            pipeline_name=pipeline_name,
            success_rate=success_rate,
            successful_runs=successful_runs,
            total_runs=total_runs
        )
        
        return Metric(
            name=f"{pipeline_name}_success_rate",
            value=success_rate,
            threshold=self.thresholds["success_rate"],
            metric_type=MetricType.PIPELINE_HEALTH,
            timestamp=datetime.now(),
            status=status,
            alert_level=alert_level
        )
    
    def check_pipeline_duration(self, duration_minutes: float, pipeline_name: str) -> Metric:
        """Check pipeline execution duration"""
        status = "healthy" if duration_minutes <= self.thresholds["avg_duration_minutes"] else "unhealthy"
        alert_level = None if status == "healthy" else AlertLevel.WARNING
        
        logger.info(
            f"⏱️ Pipeline duration for {pipeline_name}: {duration_minutes:.2f} minutes",
            pipeline_name=pipeline_name,
            duration_minutes=duration_minutes
        )
        
        return Metric(
            name=f"{pipeline_name}_duration",
            value=duration_minutes,
            threshold=self.thresholds["avg_duration_minutes"],
            metric_type=MetricType.PIPELINE_HEALTH,
            timestamp=datetime.now(),
            status=status,
            alert_level=alert_level
        )


class AlertManager:
    """Manage alerts and notifications"""
    
    def __init__(self):
        self.alerts: List[Alert] = []
        self.alert_channels = {
            AlertLevel.INFO: ["log"],
            AlertLevel.WARNING: ["log", "email"],
            AlertLevel.CRITICAL: ["log", "email", "slack"]
        }
    
    def create_alert(self, metric: Metric) -> Optional[Alert]:
        """Create alert from metric if threshold is breached"""
        if metric.alert_level is None:
            return None
        
        alert = Alert(
            id=f"{metric.name}_{int(time.time())}",
            title=f"{metric.metric_type.value.title()} Alert: {metric.name}",
            description=f"Metric {metric.name} is {metric.value:.3f}, threshold: {metric.threshold:.3f}",
            level=metric.alert_level,
            metric_name=metric.name,
            current_value=metric.value,
            threshold=metric.threshold,
            timestamp=metric.timestamp
        )
        
        self.alerts.append(alert)
        self._send_alert(alert)
        
        return alert
    
    def _send_alert(self, alert: Alert) -> None:
        """Send alert through configured channels"""
        channels = self.alert_channels.get(alert.level, ["log"])
        
        for channel in channels:
            if channel == "log":
                self._log_alert(alert)
            elif channel == "email":
                self._send_email_alert(alert)
            elif channel == "slack":
                self._send_slack_alert(alert)
    
    def _log_alert(self, alert: Alert) -> None:
        """Log alert"""
        logger.warning(
            f"🚨 {alert.level.value.upper()} ALERT: {alert.title}",
            alert_id=alert.id,
            description=alert.description,
            current_value=alert.current_value,
            threshold=alert.threshold
        )
    
    def _send_email_alert(self, alert: Alert) -> None:
        """Send email alert (placeholder)"""
        logger.info(f"📧 Email alert sent for {alert.id}")
    
    def _send_slack_alert(self, alert: Alert) -> None:
        """Send Slack alert (placeholder)"""
        logger.info(f"📱 Slack alert sent for {alert.id}")
    
    def get_active_alerts(self) -> List[Alert]:
        """Get all active (unresolved) alerts"""
        return [alert for alert in self.alerts if not alert.resolved]
    
    def resolve_alert(self, alert_id: str) -> bool:
        """Mark alert as resolved"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.resolved = True
                logger.info(f"✅ Alert {alert_id} resolved")
                return True
        return False


class MonitoringSystem:
    """Main monitoring system coordinator"""
    
    def __init__(self):
        self.data_quality_monitor = DataQualityMonitor()
        self.model_monitor = ModelPerformanceMonitor()
        self.pipeline_monitor = PipelineHealthMonitor()
        self.alert_manager = AlertManager()
        self.metrics_history: List[Metric] = []
    
    def run_monitoring_cycle(self) -> Dict[str, Any]:
        """Run a complete monitoring cycle"""
        logger.info("🔍 Starting monitoring cycle")
        
        cycle_results = {
            "timestamp": datetime.now(),
            "metrics_collected": 0,
            "alerts_generated": 0,
            "health_status": "healthy"
        }
        
        try:
            # This is a placeholder - in production, you'd collect real metrics
            # from your data layers, models, and pipeline runs
            
            # Example: Check if monitoring is working
            test_metric = Metric(
                name="monitoring_system_health",
                value=1.0,
                threshold=1.0,
                metric_type=MetricType.PIPELINE_HEALTH,
                timestamp=datetime.now(),
                status="healthy"
            )
            
            self.metrics_history.append(test_metric)
            cycle_results["metrics_collected"] = 1
            
            logger.info(
                "✅ Monitoring cycle completed successfully",
                metrics_collected=cycle_results["metrics_collected"],
                alerts_generated=cycle_results["alerts_generated"]
            )
            
        except Exception as e:
            cycle_results["health_status"] = "unhealthy"
            logger.error(f"❌ Monitoring cycle failed: {str(e)}")
        
        return cycle_results
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        if not self.metrics_history:
            return {"message": "No metrics collected yet"}
        
        recent_metrics = [m for m in self.metrics_history 
                         if m.timestamp > datetime.now() - timedelta(hours=24)]
        
        summary = {
            "total_metrics": len(self.metrics_history),
            "recent_metrics": len(recent_metrics),
            "active_alerts": len(self.alert_manager.get_active_alerts()),
            "health_status": "healthy" if all(m.status == "healthy" for m in recent_metrics) else "unhealthy"
        }
        
        return summary
