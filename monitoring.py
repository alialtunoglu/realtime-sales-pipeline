#!/usr/bin/env python3
"""
Monitoring System Entry Point
Main script to run monitoring operations
"""
import sys
import argparse
from datetime import datetime

from src.monitoring import MonitoringSystem
from src.utils.logger import logger


def run_monitoring(args):
    """Run monitoring cycle"""
    monitoring = MonitoringSystem()
    
    print("🔍 Pipeline Monitoring System")
    print("=" * 40)
    
    if args.continuous:
        print(f"🔄 Starting continuous monitoring (interval: {args.interval}s)")
        import time
        
        try:
            while True:
                result = monitoring.run_monitoring_cycle()
                
                status_icon = "✅" if result["health_status"] == "healthy" else "🚨"
                timestamp = datetime.now().strftime('%H:%M:%S')
                
                print(f"{timestamp} {status_icon} Status: {result['health_status'].upper()} "
                      f"| Metrics: {result['metrics_collected']} "
                      f"| Alerts: {result['alerts_generated']}")
                
                time.sleep(args.interval)
                
        except KeyboardInterrupt:
            print("\n🛑 Monitoring stopped by user")
    
    else:
        # Single monitoring cycle
        result = monitoring.run_monitoring_cycle()
        summary = monitoring.get_metrics_summary()
        
        # Display results
        if summary.get("health_status") == "healthy":
            print("✅ System Status: HEALTHY")
        else:
            print("🚨 System Status: UNHEALTHY")
        
        print(f"📊 Total Metrics: {summary.get('total_metrics', 0)}")
        print(f"🚨 Active Alerts: {summary.get('active_alerts', 0)}")
        print(f"⏰ Last Check: {result['timestamp'].strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Show alerts if any
        active_alerts = monitoring.alert_manager.get_active_alerts()
        if active_alerts:
            print("\n🚨 Active Alerts:")
            for alert in active_alerts:
                print(f"  - {alert.level.value.upper()}: {alert.title}")


def show_metrics(args):
    """Show collected metrics"""
    monitoring = MonitoringSystem()
    
    print("📊 Collected Metrics")
    print("=" * 60)
    
    if not monitoring.metrics_history:
        print("No metrics collected yet")
        return
    
    # Show recent metrics
    recent_metrics = monitoring.metrics_history[-args.limit:]
    
    print(f"{'Name':<25} {'Value':<10} {'Status':<10} {'Time':<20}")
    print("-" * 60)
    
    for metric in recent_metrics:
        timestamp_str = metric.timestamp.strftime('%H:%M:%S')
        print(f"{metric.name:<25} {metric.value:<10.3f} {metric.status:<10} {timestamp_str:<20}")


def show_alerts(args):
    """Show active alerts"""
    monitoring = MonitoringSystem()
    
    active_alerts = monitoring.alert_manager.get_active_alerts()
    
    print("🚨 Active Alerts")
    print("=" * 40)
    
    if not active_alerts:
        print("✅ No active alerts")
        return
    
    for alert in active_alerts:
        print(f"{alert.level.value.upper()}: {alert.title}")
        print(f"  📊 Current Value: {alert.current_value}")
        print(f"  🎯 Threshold: {alert.threshold}")
        print(f"  ⏰ Time: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"  📝 Description: {alert.description}")
        print()


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Pipeline Monitoring System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monitoring.py run                    # Single monitoring cycle
  python monitoring.py run --continuous       # Continuous monitoring
  python monitoring.py metrics                # Show metrics
  python monitoring.py alerts                 # Show alerts
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run monitoring cycle')
    run_parser.add_argument('--continuous', action='store_true', 
                           help='Run continuous monitoring')
    run_parser.add_argument('--interval', type=int, default=30,
                           help='Monitoring interval in seconds (default: 30)')
    
    # Metrics command
    metrics_parser = subparsers.add_parser('metrics', help='Show collected metrics')
    metrics_parser.add_argument('--limit', type=int, default=10,
                               help='Number of recent metrics to show (default: 10)')
    
    # Alerts command
    alerts_parser = subparsers.add_parser('alerts', help='Show active alerts')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'run':
            run_monitoring(args)
        elif args.command == 'metrics':
            show_metrics(args)
        elif args.command == 'alerts':
            show_alerts(args)
    
    except Exception as e:
        logger.error(f"❌ Monitoring failed: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
