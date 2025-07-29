"""
ML Model Evaluation Utilities
Provides comprehensive model evaluation and monitoring
"""
from typing import Dict, List, Any, Tuple
import json
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, count, stddev, min as spark_min, max as spark_max

from src.utils.logger import PipelineLogger

logger = PipelineLogger(__name__)


class ModelEvaluator:
    """Comprehensive model evaluation and metrics calculation"""
    
    def __init__(self):
        pass
    
    def evaluate_regression_model(self, predictions: DataFrame, 
                                label_col: str = "daily_revenue", 
                                prediction_col: str = "prediction") -> Dict[str, float]:
        """
        Evaluate regression model performance
        
        Args:
            predictions: DataFrame with predictions and actual values
            label_col: Name of actual values column
            prediction_col: Name of prediction column
            
        Returns:
            Dictionary of evaluation metrics
        """
        logger.info("Evaluating regression model performance")
        
        # Calculate residuals
        predictions_with_residuals = predictions.withColumn(
            "residual", 
            col(label_col) - col(prediction_col)
        ).withColumn(
            "squared_residual",
            (col(label_col) - col(prediction_col)) ** 2
        ).withColumn(
            "abs_residual",
            abs(col(label_col) - col(prediction_col))
        )
        
        # Calculate metrics
        stats = predictions_with_residuals.agg(
            count(label_col).alias("count"),
            avg("squared_residual").alias("mse"),
            avg("abs_residual").alias("mae"),
            avg(label_col).alias("mean_actual"),
            stddev(label_col).alias("std_actual")
        ).collect()[0]
        
        n = stats["count"]
        mse = stats["mse"]
        mae = stats["mae"]
        mean_actual = stats["mean_actual"]
        std_actual = stats["std_actual"]
        
        # Calculate additional metrics
        rmse = mse ** 0.5
        mape = (predictions_with_residuals.agg(
            avg(abs(col("residual") / col(label_col)) * 100).alias("mape")
        ).collect()[0]["mape"])
        
        # R-squared calculation
        ss_res = predictions_with_residuals.agg(
            sum(col("squared_residual")).alias("ss_res")
        ).collect()[0]["ss_res"]
        
        ss_tot = n * (std_actual ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0
        
        metrics = {
            "mse": float(mse),
            "rmse": float(rmse),
            "mae": float(mae),
            "mape": float(mape),
            "r_squared": float(r_squared),
            "sample_count": int(n)
        }
        
        logger.info(f"Regression evaluation completed: {metrics}")
        return metrics
    
    def evaluate_clustering_model(self, predictions: DataFrame,
                                cluster_col: str = "cluster") -> Dict[str, Any]:
        """
        Evaluate clustering model performance
        
        Args:
            predictions: DataFrame with cluster assignments
            cluster_col: Name of cluster column
            
        Returns:
            Dictionary of evaluation metrics
        """
        logger.info("Evaluating clustering model performance")
        
        # Cluster distribution
        cluster_counts = predictions.groupBy(cluster_col).count().collect()
        
        total_count = predictions.count()
        cluster_distribution = {
            f"cluster_{row[cluster_col]}": {
                "count": row["count"],
                "percentage": round((row["count"] / total_count) * 100, 2)
            }
            for row in cluster_counts
        }
        
        # Cluster balance metrics
        cluster_sizes = [row["count"] for row in cluster_counts]
        min_cluster_size = min(cluster_sizes)
        max_cluster_size = max(cluster_sizes)
        balance_ratio = min_cluster_size / max_cluster_size
        
        # Intra-cluster statistics (for features if available)
        feature_cols = ["recency", "frequency", "monetary"]
        cluster_stats = {}
        
        for cluster_id in [row[cluster_col] for row in cluster_counts]:
            cluster_data = predictions.filter(col(cluster_col) == cluster_id)
            
            stats = {}
            for feature in feature_cols:
                if feature in predictions.columns:
                    feature_stats = cluster_data.agg(
                        avg(feature).alias("mean"),
                        stddev(feature).alias("std"),
                        spark_min(feature).alias("min"),
                        spark_max(feature).alias("max")
                    ).collect()[0]
                    
                    stats[feature] = {
                        "mean": float(feature_stats["mean"] or 0),
                        "std": float(feature_stats["std"] or 0),
                        "min": float(feature_stats["min"] or 0),
                        "max": float(feature_stats["max"] or 0)
                    }
            
            cluster_stats[f"cluster_{cluster_id}"] = stats
        
        metrics = {
            "num_clusters": len(cluster_counts),
            "total_samples": total_count,
            "cluster_distribution": cluster_distribution,
            "balance_ratio": balance_ratio,
            "cluster_statistics": cluster_stats
        }
        
        logger.info(f"Clustering evaluation completed: num_clusters={len(cluster_counts)}")
        return metrics
    
    def calculate_business_impact(self, baseline_metrics: Dict[str, float],
                                current_metrics: Dict[str, float]) -> Dict[str, Any]:
        """
        Calculate business impact of model improvements
        
        Args:
            baseline_metrics: Baseline model metrics
            current_metrics: Current model metrics
            
        Returns:
            Business impact analysis
        """
        logger.info("Calculating business impact metrics")
        
        impact = {}
        
        # Calculate percentage improvements
        for metric in baseline_metrics:
            if metric in current_metrics:
                baseline = baseline_metrics[metric]
                current = current_metrics[metric]
                
                if baseline != 0:
                    improvement = ((current - baseline) / baseline) * 100
                    impact[f"{metric}_improvement_pct"] = round(improvement, 2)
                else:
                    impact[f"{metric}_improvement_pct"] = 0
        
        # Business-specific calculations
        if "mae" in baseline_metrics and "mae" in current_metrics:
            mae_improvement = baseline_metrics["mae"] - current_metrics["mae"]
            impact["forecast_accuracy_improvement"] = round(mae_improvement, 2)
        
        if "r_squared" in current_metrics:
            impact["model_reliability_score"] = round(current_metrics["r_squared"] * 100, 1)
        
        logger.info("Business impact calculation completed")
        return impact


class ModelMonitor:
    """Model performance monitoring and alerting"""
    
    def __init__(self, threshold_config: Dict[str, float] = None):
        self.thresholds = threshold_config or {
            "min_r_squared": 0.7,
            "max_mae": 1000.0,
            "max_mape": 15.0,
            "min_balance_ratio": 0.3
        }
    
    def check_model_health(self, metrics: Dict[str, float]) -> Dict[str, Any]:
        """
        Check model health against defined thresholds
        
        Args:
            metrics: Model performance metrics
            
        Returns:
            Health check results
        """
        logger.info("Performing model health check")
        
        health_status = "healthy"
        alerts = []
        passed_checks = []
        
        # Regression model checks
        if "r_squared" in metrics:
            if metrics["r_squared"] < self.thresholds["min_r_squared"]:
                health_status = "warning"
                alerts.append(f"R-squared ({metrics['r_squared']:.3f}) below threshold ({self.thresholds['min_r_squared']})")
            else:
                passed_checks.append("R-squared check passed")
        
        if "mae" in metrics:
            if metrics["mae"] > self.thresholds["max_mae"]:
                health_status = "critical"
                alerts.append(f"MAE ({metrics['mae']:.2f}) above threshold ({self.thresholds['max_mae']})")
            else:
                passed_checks.append("MAE check passed")
        
        if "mape" in metrics:
            if metrics["mape"] > self.thresholds["max_mape"]:
                health_status = "warning"
                alerts.append(f"MAPE ({metrics['mape']:.1f}%) above threshold ({self.thresholds['max_mape']}%)")
            else:
                passed_checks.append("MAPE check passed")
        
        # Clustering model checks
        if "balance_ratio" in metrics:
            if metrics["balance_ratio"] < self.thresholds["min_balance_ratio"]:
                health_status = "warning"
                alerts.append(f"Cluster balance ratio ({metrics['balance_ratio']:.3f}) below threshold ({self.thresholds['min_balance_ratio']})")
            else:
                passed_checks.append("Cluster balance check passed")
        
        health_report = {
            "timestamp": datetime.now().isoformat(),
            "status": health_status,
            "alerts": alerts,
            "passed_checks": passed_checks,
            "metrics_checked": len(metrics),
            "thresholds_used": self.thresholds
        }
        
        logger.info(f"Model health check completed: {health_status}")
        return health_report
    
    def generate_performance_report(self, evaluation_results: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate comprehensive performance report
        
        Args:
            evaluation_results: Model evaluation results
            
        Returns:
            Performance report
        """
        logger.info("Generating model performance report")
        
        report = {
            "report_timestamp": datetime.now().isoformat(),
            "model_performance": evaluation_results,
            "summary": {
                "models_evaluated": len(evaluation_results),
                "overall_health": "healthy"
            }
        }
        
        # Add health checks for each model
        health_checks = {}
        overall_alerts = []
        
        for model_name, metrics in evaluation_results.items():
            health_check = self.check_model_health(metrics)
            health_checks[model_name] = health_check
            
            if health_check["status"] != "healthy":
                overall_alerts.extend(health_check["alerts"])
                report["summary"]["overall_health"] = health_check["status"]
        
        report["health_checks"] = health_checks
        report["summary"]["total_alerts"] = len(overall_alerts)
        
        logger.info("Performance report generated successfully")
        return report
    
    def save_report(self, report: Dict[str, Any], filepath: str) -> None:
        """Save performance report to file"""
        import os
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Performance report saved to {filepath}")
