"""
Monitoring DAG for Pipeline Health Checks
Schedules regular monitoring cycles and alerts
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Import our monitoring system
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from src.monitoring import MonitoringSystem
from src.utils.logger import logger


def run_monitoring_cycle(**context):
    """Run a complete monitoring cycle"""
    monitoring = MonitoringSystem()
    
    logger.info("🔍 Starting scheduled monitoring cycle")
    
    try:
        result = monitoring.run_monitoring_cycle()
        
        logger.info(
            "✅ Monitoring cycle completed",
            metrics_collected=result["metrics_collected"],
            alerts_generated=result["alerts_generated"],
            health_status=result["health_status"]
        )
        
        # Return result for downstream tasks
        return result
        
    except Exception as e:
        logger.error(f"❌ Monitoring cycle failed: {str(e)}")
        raise


def check_active_alerts(**context):
    """Check for active alerts and take action if needed"""
    monitoring = MonitoringSystem()
    
    active_alerts = monitoring.alert_manager.get_active_alerts()
    
    if active_alerts:
        logger.warning(f"🚨 Found {len(active_alerts)} active alerts")
        
        for alert in active_alerts:
            logger.warning(
                f"Alert: {alert.title}",
                alert_id=alert.id,
                level=alert.level.value,
                current_value=alert.current_value,
                threshold=alert.threshold
            )
        
        # Could trigger additional actions here like sending notifications
        
    else:
        logger.info("✅ No active alerts")
    
    return len(active_alerts)


def generate_health_report(**context):
    """Generate and log health report"""
    monitoring = MonitoringSystem()
    
    summary = monitoring.get_metrics_summary()
    
    logger.info(
        "📊 Health Report Generated",
        **summary
    )
    
    return summary


# Default DAG arguments
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 29),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

# Create the DAG
dag = DAG(
    'monitoring_pipeline',
    default_args=default_args,
    description='Pipeline monitoring and health checks',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    max_active_runs=1,
    tags=['monitoring', 'health-check', 'alerts']
)

# Task 1: Run monitoring cycle
monitoring_task = PythonOperator(
    task_id='run_monitoring_cycle',
    python_callable=run_monitoring_cycle,
    dag=dag,
    doc_md="""
    ## Run Monitoring Cycle
    
    Executes a complete monitoring cycle including:
    - Data quality checks
    - Model performance monitoring
    - Pipeline health verification
    - Metric collection
    """
)

# Task 2: Check for alerts
alert_check_task = PythonOperator(
    task_id='check_active_alerts',
    python_callable=check_active_alerts,
    dag=dag,
    doc_md="""
    ## Check Active Alerts
    
    Reviews all active alerts and logs details.
    Could be extended to send notifications or trigger remediation.
    """
)

# Task 3: Generate health report
health_report_task = PythonOperator(
    task_id='generate_health_report',
    python_callable=generate_health_report,
    dag=dag,
    doc_md="""
    ## Generate Health Report
    
    Creates a comprehensive health report with metrics summary.
    """
)

# Task 4: CLI health check (optional verification)
cli_health_check = BashOperator(
    task_id='cli_health_check',
    bash_command='cd /Users/alialtunoglu/Desktop/realtime-sales-pipeline && python -m src.monitoring.cli health-check',
    dag=dag,
    doc_md="""
    ## CLI Health Check
    
    Runs the monitoring CLI health check command as a verification step.
    """
)

# Set task dependencies
monitoring_task >> [alert_check_task, health_report_task]
health_report_task >> cli_health_check
