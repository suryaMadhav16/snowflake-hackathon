import streamlit as st
import pandas as pd
import plotly.express as px
from typing import List, Dict, Optional
from crawl4ai import CrawlResult
from urllib.parse import urlparse
from datetime import datetime

class ResultsDisplay:
    """Handles display and analysis of crawling results"""
    
    def __init__(self):
        # Initialize session state variables
        if 'current_results' not in st.session_state:
            st.session_state.current_results = []
            
        if 'result_counter' not in st.session_state:
            st.session_state.result_counter = 0
            
        # Create static containers
        st.markdown("## Results")
        self.metrics_container = st.container()
        self.progress_container = st.container()
        self.details_container = st.container()
    
    def _process_results_data(self, results: List[CrawlResult]) -> pd.DataFrame:
        """Convert results to DataFrame for analysis"""
        data = []
        for result in results:
            parsed = urlparse(result.url)
            
            metrics = {
                'url': result.url,
                'domain': parsed.netloc,
                'path': parsed.path,
                'success': result.success,
                'timestamp': datetime.now(),
                'image_count': len(result.media.get('images', [])) if result.media else 0,
                'link_count': len(result.links.get('internal', [])) if result.links else 0
            }
            
            data.append(metrics)
        
        return pd.DataFrame(data)
    
    def _render_metrics(self, df: pd.DataFrame):
        """Render metrics summary"""
        with self.metrics_container:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric(
                    "Total URLs",
                    len(df),
                    f"{len(df[df['success']]) / len(df):.1%} Success"
                )
            
            with col2:
                st.metric(
                    "Unique Domains",
                    df['domain'].nunique()
                )
            
            with col3:
                st.metric(
                    "Average Links/Page",
                    f"{df['link_count'].mean():.1f}"
                )
    
    def _render_progress(self, df: pd.DataFrame):
        """Render progress visualization"""
        with self.progress_container:
            st.markdown("### Processing Progress")
            if not df.empty:
                fig = px.line(
                    df.sort_values('timestamp').reset_index(),
                    x='timestamp',
                    y=df['success'].cumsum() / (df.index + 1),
                    title="Success Rate Over Time",
                    labels={'y': 'Success Rate', 'timestamp': 'Time'},
                )
                fig.update_layout(
                    yaxis_tickformat='.1%',
                    hovermode='x unified'
                )
                st.plotly_chart(fig, use_container_width=True)
            
                # Add summary statistics
                success_rate = len(df[df['success']]) / len(df)
                st.progress(success_rate, text=f"Overall Success Rate: {success_rate:.1%}")
    
    def _render_details(self, df: pd.DataFrame):
        """Render detailed results view"""
        with self.details_container:
            with st.expander("View Detailed Results", expanded=False):
                col1, col2 = st.columns([1, 2])
                
                with col1:
                    domain_filter = st.selectbox(
                        "Filter by Domain",
                        ["All"] + list(df['domain'].unique()),
                        key=f"domain_filter_{st.session_state.result_counter}"
                    )
                
                filtered_df = df if domain_filter == "All" else df[df['domain'] == domain_filter]
                
                if not filtered_df.empty:
                    st.dataframe(
                        filtered_df[[
                            'url', 'success', 'image_count', 
                            'link_count', 'timestamp'
                        ]].sort_values('timestamp', ascending=False),
                        use_container_width=True,
                        column_config={
                            'url': 'URL',
                            'success': 'Success',
                            'image_count': 'Images',
                            'link_count': 'Links',
                            'timestamp': 'Time'
                        }
                    )
                else:
                    st.info("No results match the selected filter")
    
    def render_results_summary(self, results: List[CrawlResult]):
        """Render summary of crawling results"""
        if not results and not st.session_state.current_results:
            return
        
        # Update cumulative results
        st.session_state.current_results.extend(results)
        st.session_state.result_counter += 1
        
        # Create DataFrame from all results
        df = self._process_results_data(st.session_state.current_results)
        
        # Update all sections
        self._render_metrics(df)
        self._render_progress(df)
        self._render_details(df)
    
    def render_error_analysis(self, results: List[CrawlResult]):
        """Render analysis of crawling errors"""
        failed_results = [r for r in results if not r.success]
        
        if failed_results:
            with st.expander("Error Analysis", expanded=False):
                error_df = pd.DataFrame([
                    {
                        'url': r.url,
                        'error': r.error_message,
                        'timestamp': datetime.now()
                    }
                    for r in failed_results
                ])
                
                st.dataframe(
                    error_df.sort_values('timestamp', ascending=False),
                    use_container_width=True,
                    column_config={
                        'url': 'URL',
                        'error': 'Error Message',
                        'timestamp': 'Time'
                    }
                )
    
    def clear_results(self):
        """Clear current results"""
        st.session_state.current_results = []
        st.session_state.result_counter = 0
