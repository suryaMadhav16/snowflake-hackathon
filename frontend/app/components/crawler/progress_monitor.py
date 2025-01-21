import streamlit as st
from typing import Dict, Optional
import asyncio
import time

class ProgressMonitor:
    """Component for monitoring crawl progress and metrics"""
    
    def __init__(self, task_id: str):
        """Initialize progress monitor"""
        self.task_id = task_id
        
        # Create containers
        self.connection_status = st.empty()
        self.progress_container = st.container()
        self.results_container = st.container()
        
        with self.progress_container:
            self.progress_bar = st.empty()
            self.status_text = st.empty()
            self.metrics_container = st.empty()
            self.content_metrics = st.empty()
        
        self.last_update = time.time()
        
        # Initialize session state for this task
        if f"task_{task_id}_progress" not in st.session_state:
            st.session_state[f"task_{task_id}_progress"] = {
                "progress": 0.0,
                "status": "Starting...",
                "metrics": {
                    "successful": 0,
                    "failed": 0,
                    "skipped": 0,
                    "urls_per_second": 0.0,
                    "memory_usage": 0,
                    "saved_content": {
                        "markdown": 0,
                        "images": 0,
                        "pdf": 0,
                        "screenshot": 0
                    }
                }
            }
    
    def show_connection_status(self):
        """Show WebSocket connection status"""
        try:
            ws_client = st.session_state.ws_client
            progress_status = ws_client.get_connection_status("progress")
            metrics_status = ws_client.get_connection_status("metrics")
            
            with self.connection_status:
                st.sidebar.subheader("🔌 Connection Status")
                
                # Progress WebSocket
                progress_color = "🟢" if progress_status["connected"] else "🔴"
                st.sidebar.write(f"{progress_color} Progress WebSocket")
                if not progress_status["connected"] and progress_status["last_error"]:
                    st.sidebar.error(progress_status["last_error"])
                
                # Metrics WebSocket
                metrics_color = "🟢" if metrics_status["connected"] else "🔴"
                st.sidebar.write(f"{metrics_color} Metrics WebSocket")
                if not metrics_status["connected"] and metrics_status["last_error"]:
                    st.sidebar.error(metrics_status["last_error"])
        except Exception as e:
            st.sidebar.error(f"Error showing connection status: {str(e)}")
    
    def update_progress(self, data: Dict):
        """Update progress bar and status"""
        progress = data.get("progress", 0)
        status = data.get("status", "In progress...")
        
        # Update session state
        st.session_state[f"task_{self.task_id}_progress"]["progress"] = progress
        st.session_state[f"task_{self.task_id}_progress"]["status"] = status
        
        # Update UI
        self.progress_bar.progress(progress)
        self.status_text.write(f"📊 **Status:** {status}")
        
        # Show connection status
        self.show_connection_status()
        
        # Show phase message
        if progress < 1.0:
            if status == "Starting...":
                st.info("🔍 Discovering URLs... Please wait...")
            elif "crawling" in status.lower():
                st.info("🌐 Crawling discovered URLs...")
    
    def update_metrics(self, data: Dict):
        """Update metrics display"""
        metrics = data.get("metrics", {})
        
        # Update session state
        st.session_state[f"task_{self.task_id}_progress"]["metrics"] = metrics
        
        # Update UI
        with self.metrics_container:
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "✅ Successful",
                    metrics.get("successful", 0),
                    delta=metrics.get("successful", 0) - self._get_last_metric("successful")
                )
            
            with col2:
                st.metric(
                    "❌ Failed",
                    metrics.get("failed", 0),
                    delta=metrics.get("failed", 0) - self._get_last_metric("failed"),
                    delta_color="inverse"
                )
            
            with col3:
                st.metric(
                    "⚡ URLs/sec",
                    f"{metrics.get('urls_per_second', 0):.1f}",
                    delta=None
                )
            
            with col4:
                st.metric(
                    "💾 Memory (MB)",
                    f"{metrics.get('memory_usage', 0) / 1024 / 1024:.1f}",
                    delta=None
                )
        
        # Update content metrics
        saved_content = metrics.get("saved_content", {})
        with self.content_metrics:
            st.subheader("📦 Saved Content")
            col5, col6, col7, col8 = st.columns(4)
            
            with col5:
                st.metric(
                    "📝 Markdown",
                    saved_content.get("markdown", 0)
                )
            
            with col6:
                st.metric(
                    "🖼️ Images",
                    saved_content.get("images", 0)
                )
            
            with col7:
                st.metric(
                    "📄 PDFs",
                    saved_content.get("pdf", 0)
                )
            
            with col8:
                st.metric(
                    "📸 Screenshots",
                    saved_content.get("screenshot", 0)
                )
    
    def _get_last_metric(self, key: str) -> int:
        """Get last value of a metric"""
        try:
            last_metrics = st.session_state[f"task_{self.task_id}_progress"]["metrics"]
            return last_metrics.get(key, 0)
        except Exception:
            return 0
    
    def get_current_progress(self) -> float:
        """Get current progress value"""
        try:
            return st.session_state[f"task_{self.task_id}_progress"]["progress"]
        except Exception:
            return 0.0
    
    def get_current_status(self) -> str:
        """Get current status text"""
        try:
            return st.session_state[f"task_{self.task_id}_progress"]["status"]
        except Exception:
            return "Unknown"
    
    def is_complete(self) -> bool:
        """Check if task is complete"""
        return self.get_current_progress() >= 1.0 or \
               self.get_current_status().lower() in ["completed", "failed"]
    
    def cleanup(self):
        """Clean up session state"""
        if f"task_{self.task_id}_progress" in st.session_state:
            del st.session_state[f"task_{self.task_id}_progress"]
        
        # Clear UI containers
        self.connection_status.empty()
        with self.progress_container:
            self.progress_bar.empty()
            self.status_text.empty()
            self.metrics_container.empty()
            self.content_metrics.empty()
