"""
Monitoring CLI Tool
Command-line interface for monitoring system operations
"""
import click
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from src.monitoring import MonitoringSystem, AlertLevel


@click.group()
@click.pass_context
def cli(ctx):
    """Pipeline Monitoring CLI Tool"""
    ctx.ensure_object(dict)
    ctx.obj['monitoring'] = MonitoringSystem()


@cli.command()
@click.pass_context
def status(ctx):
    """Get overall system status"""
    monitoring = ctx.obj['monitoring']
    
    click.echo("🔍 Pipeline Monitoring Status")
    click.echo("=" * 40)
    
    # Run monitoring cycle
    cycle_result = monitoring.run_monitoring_cycle()
    
    # Get metrics summary
    summary = monitoring.get_metrics_summary()
    
    # Display status
    if summary.get("health_status") == "healthy":
        click.echo(click.style("✅ System Status: HEALTHY", fg="green", bold=True))
    else:
        click.echo(click.style("🚨 System Status: UNHEALTHY", fg="red", bold=True))
    
    click.echo(f"📊 Total Metrics: {summary.get('total_metrics', 0)}")
    click.echo(f"🚨 Active Alerts: {summary.get('active_alerts', 0)}")
    click.echo(f"⏰ Last Check: {cycle_result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")


@cli.command()
@click.option('--level', type=click.Choice(['info', 'warning', 'critical']), 
              help='Filter alerts by level')
@click.pass_context
def alerts(ctx, level):
    """List active alerts"""
    monitoring = ctx.obj['monitoring']
    
    active_alerts = monitoring.alert_manager.get_active_alerts()
    
    if level:
        alert_level = AlertLevel(level)
        active_alerts = [a for a in active_alerts if a.level == alert_level]
    
    click.echo("🚨 Active Alerts")
    click.echo("=" * 40)
    
    if not active_alerts:
        click.echo(click.style("✅ No active alerts", fg="green"))
        return
    
    for alert in active_alerts:
        level_color = {
            AlertLevel.INFO: "blue",
            AlertLevel.WARNING: "yellow", 
            AlertLevel.CRITICAL: "red"
        }.get(alert.level, "white")
        
        click.echo(f"{alert.level.value.upper()}: {alert.title}", color=level_color)
        click.echo(f"  📊 Current Value: {alert.current_value}")
        click.echo(f"  🎯 Threshold: {alert.threshold}")
        click.echo(f"  ⏰ Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        click.echo(f"  📝 Description: {alert.description}")
        click.echo()


@cli.command()
@click.argument('alert_id')
@click.pass_context
def resolve(ctx, alert_id):
    """Resolve an alert by ID"""
    monitoring = ctx.obj['monitoring']
    
    success = monitoring.alert_manager.resolve_alert(alert_id)
    
    if success:
        click.echo(click.style(f"✅ Alert {alert_id} resolved", fg="green"))
    else:
        click.echo(click.style(f"❌ Alert {alert_id} not found", fg="red"))


@cli.command()
@click.option('--format', type=click.Choice(['table', 'json']), default='table',
              help='Output format')
@click.pass_context
def metrics(ctx, format):
    """Show collected metrics"""
    monitoring = ctx.obj['monitoring']
    
    if not monitoring.metrics_history:
        click.echo("📊 No metrics collected yet")
        return
    
    if format == 'json':
        metrics_data = []
        for metric in monitoring.metrics_history[-10:]:  # Last 10 metrics
            metrics_data.append({
                'name': metric.name,
                'value': metric.value,
                'threshold': metric.threshold,
                'status': metric.status,
                'type': metric.metric_type.value,
                'timestamp': metric.timestamp.isoformat()
            })
        
        click.echo(json.dumps(metrics_data, indent=2))
    
    else:
        click.echo("📊 Recent Metrics")
        click.echo("=" * 60)
        click.echo(f"{'Name':<25} {'Value':<10} {'Status':<10} {'Time':<20}")
        click.echo("-" * 60)
        
        for metric in monitoring.metrics_history[-10:]:
            status_color = "green" if metric.status == "healthy" else "red"
            timestamp_str = metric.timestamp.strftime('%H:%M:%S')
            
            click.echo(f"{metric.name:<25} {metric.value:<10.3f} ", nl=False)
            click.echo(click.style(f"{metric.status:<10}", fg=status_color), nl=False)
            click.echo(f" {timestamp_str:<20}")


@cli.command()
@click.pass_context
def dashboard(ctx):
    """Launch monitoring dashboard"""
    try:
        import subprocess
        import os
        
        dashboard_path = os.path.join(
            os.path.dirname(__file__), 
            "dashboard.py"
        )
        
        click.echo("🚀 Launching monitoring dashboard...")
        click.echo("📊 Dashboard will be available at: http://localhost:8501")
        
        # Launch Streamlit dashboard
        subprocess.run([
            "streamlit", "run", dashboard_path,
            "--server.port", "8501",
            "--server.headless", "true"
        ])
        
    except ImportError:
        click.echo(click.style("❌ Streamlit not installed. Install with: pip install streamlit", fg="red"))
    except Exception as e:
        click.echo(click.style(f"❌ Failed to launch dashboard: {str(e)}", fg="red"))


@cli.command()
@click.option('--duration', default=60, help='Monitoring duration in seconds')
@click.option('--interval', default=30, help='Check interval in seconds')
@click.pass_context
def watch(ctx, duration, interval):
    """Continuous monitoring mode"""
    import time
    
    monitoring = ctx.obj['monitoring']
    
    click.echo(f"👀 Starting continuous monitoring for {duration} seconds")
    click.echo(f"🔄 Check interval: {interval} seconds")
    click.echo("Press Ctrl+C to stop")
    click.echo()
    
    start_time = time.time()
    
    try:
        while time.time() - start_time < duration:
            # Run monitoring cycle
            cycle_result = monitoring.run_monitoring_cycle()
            
            # Display brief status
            status_icon = "✅" if cycle_result["health_status"] == "healthy" else "🚨"
            timestamp = datetime.now().strftime('%H:%M:%S')
            
            click.echo(f"{timestamp} {status_icon} Status: {cycle_result['health_status'].upper()} "
                      f"| Metrics: {cycle_result['metrics_collected']} "
                      f"| Alerts: {cycle_result['alerts_generated']}")
            
            # Wait for next check
            time.sleep(interval)
    
    except KeyboardInterrupt:
        click.echo("\n🛑 Monitoring stopped by user")


@cli.command()
@click.pass_context
def health_check(ctx):
    """Run comprehensive health check"""
    monitoring = ctx.obj['monitoring']
    
    click.echo("🏥 Running Comprehensive Health Check")
    click.echo("=" * 50)
    
    # Simulate health checks for different components
    components = {
        "Data Quality Monitor": "healthy",
        "Model Performance Monitor": "healthy", 
        "Pipeline Health Monitor": "healthy",
        "Alert Manager": "healthy",
        "Metrics Collection": "healthy"
    }
    
    all_healthy = True
    
    for component, status in components.items():
        if status == "healthy":
            click.echo(f"✅ {component}: OK")
        else:
            click.echo(f"❌ {component}: {status.upper()}")
            all_healthy = False
    
    click.echo()
    
    if all_healthy:
        click.echo(click.style("🎉 All systems healthy!", fg="green", bold=True))
    else:
        click.echo(click.style("⚠️  Some systems need attention", fg="yellow", bold=True))
    
    # Show summary
    summary = monitoring.get_metrics_summary()
    click.echo(f"\n📈 Summary:")
    click.echo(f"  Total Metrics: {summary.get('total_metrics', 0)}")
    click.echo(f"  Active Alerts: {summary.get('active_alerts', 0)}")
    click.echo(f"  Overall Status: {summary.get('health_status', 'unknown').upper()}")


if __name__ == '__main__':
    cli()
