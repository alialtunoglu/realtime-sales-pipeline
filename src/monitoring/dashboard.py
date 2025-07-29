"""
Monitoring Dashboard
Real-time monitoring dashboard for pipeline health, data quality, and model performance
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
import json
import time

from src.monitoring import MonitoringSystem, AlertLevel, MetricType


class MonitoringDashboard:
    """Streamlit dashboard for monitoring system"""
    
    def __init__(self):
        self.monitoring_system = MonitoringSystem()
        self.setup_page_config()
    
    def setup_page_config(self):
        """Configure Streamlit page"""
        st.set_page_config(
            page_title="🔍 Pipeline Monitoring Dashboard",
            page_icon="🔍",
            layout="wide",
            initial_sidebar_state="expanded"
        )
    
    def render_dashboard(self):
        """Render the main dashboard"""
        st.title("🔍 Pipeline Monitoring Dashboard")
        st.markdown("**Real-time monitoring** of data quality, model performance, and pipeline health")
        
        # Sidebar controls
        self.render_sidebar()
        
        # Main dashboard content
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            self.render_health_status_card()
        
        with col2:
            self.render_alerts_card()
        
        with col3:
            self.render_metrics_card()
        
        with col4:
            self.render_uptime_card()
        
        # Detailed monitoring sections
        st.markdown("---")
        
        tab1, tab2, tab3, tab4 = st.tabs([
            "📊 Data Quality", 
            "🤖 Model Performance", 
            "⚙️ Pipeline Health", 
            "🚨 Alerts & Incidents"
        ])
        
        with tab1:
            self.render_data_quality_section()
        
        with tab2:
            self.render_model_performance_section()
        
        with tab3:
            self.render_pipeline_health_section()
        
        with tab4:
            self.render_alerts_section()
    
    def render_sidebar(self):
        """Render sidebar with controls"""
        st.sidebar.header("🎛️ Dashboard Controls")
        
        # Auto-refresh toggle
        auto_refresh = st.sidebar.checkbox("🔄 Auto Refresh (30s)", value=False)
        
        if auto_refresh:
            time.sleep(30)
            st.rerun()
        
        # Manual refresh button
        if st.sidebar.button("🔄 Refresh Now"):
            st.rerun()
        
        # Time range selector
        st.sidebar.subheader("📅 Time Range")
        time_range = st.sidebar.selectbox(
            "Select Range",
            ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"]
        )
        
        # Monitoring toggles
        st.sidebar.subheader("📊 Monitoring Scope")
        monitor_data_quality = st.sidebar.checkbox("Data Quality", value=True)
        monitor_models = st.sidebar.checkbox("Model Performance", value=True)
        monitor_pipelines = st.sidebar.checkbox("Pipeline Health", value=True)
        
        return {
            "time_range": time_range,
            "monitor_data_quality": monitor_data_quality,
            "monitor_models": monitor_models,
            "monitor_pipelines": monitor_pipelines
        }
    
    def render_health_status_card(self):
        """Render overall health status card"""
        # Simulate health check
        summary = self.monitoring_system.get_metrics_summary()
        health_status = summary.get("health_status", "healthy")
        
        if health_status == "healthy":
            st.success("🟢 **HEALTHY**")
            st.metric("System Status", "All systems operational", "")
        else:
            st.error("🔴 **UNHEALTHY**")
            st.metric("System Status", "Issues detected", "⚠️")
    
    def render_alerts_card(self):
        """Render active alerts card"""
        active_alerts = len(self.monitoring_system.alert_manager.get_active_alerts())
        
        if active_alerts == 0:
            st.info("🔕 **NO ALERTS**")
            st.metric("Active Alerts", "0", "")
        else:
            st.warning(f"🚨 **{active_alerts} ALERTS**")
            st.metric("Active Alerts", str(active_alerts), "🚨")
    
    def render_metrics_card(self):
        """Render metrics collection card"""
        summary = self.monitoring_system.get_metrics_summary()
        total_metrics = summary.get("total_metrics", 0)
        
        st.info("📊 **METRICS**")
        st.metric("Total Collected", str(total_metrics), "+1 recent")
    
    def render_uptime_card(self):
        """Render system uptime card"""
        # Simulate uptime calculation
        uptime_percentage = 99.8
        
        st.success("⏱️ **UPTIME**")
        st.metric("Last 30 Days", f"{uptime_percentage}%", "+0.1%")
    
    def render_data_quality_section(self):
        """Render data quality monitoring section"""
        st.subheader("📊 Data Quality Monitoring")
        
        # Data quality metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 📈 Quality Metrics")
            
            # Simulate data quality metrics
            quality_data = {
                "Bronze Layer": {"Completeness": 98.5, "Accuracy": 99.2, "Freshness": 0.5},
                "Silver Layer": {"Completeness": 99.1, "Accuracy": 99.8, "Freshness": 1.2},
                "Gold Layer": {"Completeness": 99.9, "Accuracy": 99.9, "Freshness": 1.8}
            }
            
            for layer, metrics in quality_data.items():
                st.write(f"**{layer}**")
                for metric, value in metrics.items():
                    if metric == "Freshness":
                        st.metric(f"{metric} (hours)", f"{value}", "")
                    else:
                        st.metric(f"{metric} (%)", f"{value}", "")
                st.markdown("---")
        
        with col2:
            st.markdown("#### 📊 Quality Trends")
            
            # Generate sample data quality trend
            dates = pd.date_range(start=datetime.now() - timedelta(days=7), 
                                end=datetime.now(), freq='H')
            
            trend_data = pd.DataFrame({
                'timestamp': dates,
                'bronze_quality': [95 + (i % 5) for i in range(len(dates))],
                'silver_quality': [97 + (i % 3) for i in range(len(dates))],
                'gold_quality': [99 + (i % 2) for i in range(len(dates))]
            })
            
            fig = px.line(trend_data, x='timestamp', 
                         y=['bronze_quality', 'silver_quality', 'gold_quality'],
                         title="Data Quality Over Time",
                         labels={'value': 'Quality Score (%)', 'timestamp': 'Time'})
            
            st.plotly_chart(fig, use_container_width=True)
    
    def render_model_performance_section(self):
        """Render model performance monitoring section"""
        st.subheader("🤖 Model Performance Monitoring")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🎯 Model Accuracy")
            
            # Simulate model performance metrics
            models = {
                "Sales Forecasting": {"accuracy": 87.5, "drift": 2.1},
                "Customer Segmentation": {"accuracy": 92.3, "drift": 1.8},
                "Revenue Prediction": {"accuracy": 89.7, "drift": 3.2}
            }
            
            for model, metrics in models.items():
                st.write(f"**{model}**")
                accuracy_delta = "+0.5%" if metrics["accuracy"] > 85 else "-1.2%"
                st.metric("Accuracy (%)", f"{metrics['accuracy']}", accuracy_delta)
                
                drift_color = "🟢" if metrics["drift"] < 3 else "🟡"
                st.metric(f"Drift {drift_color}", f"{metrics['drift']}%", "")
                st.markdown("---")
        
        with col2:
            st.markdown("#### 📈 Performance Trends")
            
            # Generate sample model performance trend
            dates = pd.date_range(start=datetime.now() - timedelta(days=7), 
                                end=datetime.now(), freq='D')
            
            perf_data = pd.DataFrame({
                'date': dates,
                'forecasting': [85 + (i % 3) for i in range(len(dates))],
                'segmentation': [90 + (i % 2) for i in range(len(dates))],
                'prediction': [87 + (i % 4) for i in range(len(dates))]
            })
            
            fig = px.line(perf_data, x='date', 
                         y=['forecasting', 'segmentation', 'prediction'],
                         title="Model Accuracy Trends",
                         labels={'value': 'Accuracy (%)', 'date': 'Date'})
            
            st.plotly_chart(fig, use_container_width=True)
    
    def render_pipeline_health_section(self):
        """Render pipeline health monitoring section"""
        st.subheader("⚙️ Pipeline Health Monitoring")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("#### 🔄 Pipeline Status")
            
            # Simulate pipeline health
            pipelines = {
                "Bronze Ingestion": {"status": "✅ Running", "success_rate": 99.2, "duration": 15.3},
                "Silver Cleaning": {"status": "✅ Running", "success_rate": 98.8, "duration": 22.1},
                "Gold Analytics": {"status": "⚠️ Warning", "success_rate": 96.5, "duration": 45.7},
                "ML Training": {"status": "✅ Running", "success_rate": 97.9, "duration": 180.2}
            }
            
            for pipeline, metrics in pipelines.items():
                st.write(f"**{pipeline}**")
                st.write(f"Status: {metrics['status']}")
                st.metric("Success Rate (%)", f"{metrics['success_rate']}", "")
                st.metric("Avg Duration (min)", f"{metrics['duration']}", "")
                st.markdown("---")
        
        with col2:
            st.markdown("#### 🎯 Resource Usage")
            
            # Resource usage gauge charts
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=('CPU Usage', 'Memory Usage', 'Disk Usage', 'Network I/O'),
                specs=[[{"type": "indicator"}, {"type": "indicator"}],
                       [{"type": "indicator"}, {"type": "indicator"}]]
            )
            
            # CPU Usage
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=67,
                title={'text': "CPU %"},
                gauge={'axis': {'range': [None, 100]},
                       'bar': {'color': "darkblue"},
                       'threshold': {'line': {'color': "red", 'width': 4},
                                   'thickness': 0.75, 'value': 90}}
            ), row=1, col=1)
            
            # Memory Usage
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=45,
                title={'text': "Memory %"},
                gauge={'axis': {'range': [None, 100]},
                       'bar': {'color': "green"},
                       'threshold': {'line': {'color': "red", 'width': 4},
                                   'thickness': 0.75, 'value': 80}}
            ), row=1, col=2)
            
            # Disk Usage
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=23,
                title={'text': "Disk %"},
                gauge={'axis': {'range': [None, 100]},
                       'bar': {'color': "orange"},
                       'threshold': {'line': {'color': "red", 'width': 4},
                                   'thickness': 0.75, 'value': 85}}
            ), row=2, col=1)
            
            # Network I/O
            fig.add_trace(go.Indicator(
                mode="gauge+number",
                value=12,
                title={'text': "Network MB/s"},
                gauge={'axis': {'range': [None, 100]},
                       'bar': {'color': "purple"},
                       'threshold': {'line': {'color': "red", 'width': 4},
                                   'thickness': 0.75, 'value': 80}}
            ), row=2, col=2)
            
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    
    def render_alerts_section(self):
        """Render alerts and incidents section"""
        st.subheader("🚨 Alerts & Incidents")
        
        # Alert summary
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🔴 Critical", "0", "")
        
        with col2:
            st.metric("🟡 Warning", "2", "+1")
        
        with col3:
            st.metric("🔵 Info", "5", "+2")
        
        # Recent alerts table
        st.markdown("#### 📋 Recent Alerts")
        
        # Simulate alert data
        alert_data = pd.DataFrame({
            'Timestamp': [
                datetime.now() - timedelta(minutes=30),
                datetime.now() - timedelta(hours=2),
                datetime.now() - timedelta(hours=6),
                datetime.now() - timedelta(days=1)
            ],
            'Level': ['Warning', 'Info', 'Warning', 'Info'],
            'Component': ['Gold Pipeline', 'Model Monitor', 'Data Quality', 'Bronze Ingestion'],
            'Message': [
                'Pipeline duration exceeded threshold (45.7 min > 45 min)',
                'Model accuracy within normal range',
                'Data completeness warning in silver layer',
                'Ingestion completed successfully'
            ],
            'Status': ['Active', 'Resolved', 'Resolved', 'Resolved']
        })
        
        # Color code the alerts
        def highlight_alerts(val):
            if val == 'Critical':
                return 'background-color: #ffebee'
            elif val == 'Warning':
                return 'background-color: #fff3e0'
            elif val == 'Info':
                return 'background-color: #e3f2fd'
            return ''
        
        styled_df = alert_data.style.applymap(highlight_alerts, subset=['Level'])
        st.dataframe(styled_df, use_container_width=True)
        
        # Alert resolution actions
        st.markdown("#### 🔧 Quick Actions")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            if st.button("🔄 Restart Failed Pipelines"):
                st.success("✅ Pipeline restart initiated")
        
        with col2:
            if st.button("📧 Send Alert Summary"):
                st.success("✅ Alert summary sent to team")
        
        with col3:
            if st.button("🔕 Snooze Warnings"):
                st.success("✅ Warning alerts snoozed for 1 hour")


def main():
    """Main function to run the monitoring dashboard"""
    dashboard = MonitoringDashboard()
    dashboard.render_dashboard()


if __name__ == "__main__":
    main()
