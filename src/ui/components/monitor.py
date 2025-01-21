import streamlit as st
import plotly.graph_objects as go
from typing import Dict, List
from datetime import datetime
import pandas as pd

class CrawlerMonitor:
    """Handles progress monitoring and metrics display"""
    
    def __init__(self):
        if 'metrics_history' not in st.session_state:
            st.session_state.metrics_history = []
    
    def _create_progress_bar(self, total: int, current: int):
        """Create or update the progress bar"""
        if 'progress_bar' not in st.session_state:
            st.session_state.progress_bar = st.progress(0)
        
        progress = current / total if total > 0 else 0
        st.session_state.progress_bar.progress(progress)
        
        return st.empty()
    
    def _create_metrics_chart(self, metrics_history: List[Dict]):
        """Create metrics visualization"""
        if not metrics_history:
            return
        
        df = pd.DataFrame(metrics_history)
        
        # Create success rate chart
        fig = go.Figure()
        
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['success_rate'],
            mode='lines+markers',
            name='Success Rate',
            line=dict(color='green')
        ))
        
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['urls_per_second'],
            mode='lines+markers',
            name='URLs/Second',
            line=dict(color='blue'),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Crawling Performance Metrics',
            xaxis=dict(title='Time'),
            yaxis=dict(
                title='Success Rate (%)',
                tickformat=',.1%',
                range=[0, 1]
            ),
            yaxis2=dict(
                title='URLs/Second',
                overlaying='y',
                side='right'
            ),
            height=300
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    def update_metrics(self, metrics: Dict):
        """Update metrics history"""
        metrics['timestamp'] = datetime.now()
        metrics['success_rate'] = (
            metrics['successful'] / (metrics['successful'] + metrics['failed'])
            if (metrics['successful'] + metrics['failed']) > 0 else 0
        )
        
        st.session_state.metrics_history.append(metrics)
        
        # Keep only last 100 data points
        if len(st.session_state.metrics_history) > 100:
            st.session_state.metrics_history.pop(0)
    
    def render_progress(self, total_urls: int, processed_urls: int, current_metrics: Dict):
        """Render progress monitoring UI"""
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric(
                "URLs Processed",
                f"{processed_urls}/{total_urls}",
                f"{(processed_urls/total_urls*100):.1f}%" if total_urls > 0 else "0%"
            )
        
        with col2:
            st.metric(
                "Success Rate",
                f"{(current_metrics.get('success_rate', 0)*100):.1f}%",
                f"{current_metrics.get('urls_per_second', 0):.1f} URLs/s"
            )
        
        with col3:
            st.metric(
                "Memory Usage",
                f"{current_metrics.get('memory_usage', 0):.0f} MB",
                f"{current_metrics.get('memory_change', 0):.1f} MB"
            )
        
        # Update progress bar
        status = self._create_progress_bar(total_urls, processed_urls)
        
        # Show current status
        if processed_urls < total_urls:
            status.info(f"Processing batch {current_metrics.get('current_batch', 0)}")
        else:
            status.success("Crawling completed!")
        
        # Show metrics chart
        self._create_metrics_chart(st.session_state.metrics_history)
    
    def render_error_summary(self, errors: List[Dict]):
        """Render error summary"""
        if errors:
            st.error("Crawling Errors")
            for error in errors:
                st.write(f"- {error['url']}: {error['message']}")
    
    def clear_metrics(self):
        """Clear metrics history"""
        st.session_state.metrics_history = []
        if 'progress_bar' in st.session_state:
            del st.session_state.progress_bar