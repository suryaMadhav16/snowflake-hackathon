import streamlit as st
from typing import Dict, Optional
import time
import datetime

class ProgressMonitor:
    """Component for monitoring crawl progress and metrics"""
    
    def __init__(self, task_type: str):
        """Initialize progress monitor"""
        self.task_type = task_type
        
        # Create containers
        self.status_container = st.container()
        self.progress_container = st.container()
        
        with self.progress_container:
            self.progress_bar = st.empty()
            self.status_text = st.empty()
            self.metrics_container = st.empty()
            self.content_metrics = st.empty()
        
        self.last_update = time.time()
        
        # Initialize state
        if f"task_{task_type}_progress" not in st.session_state:
            st.session_state[f"task_{task_type}_progress"] = {
                "progress": 0.0,
                "status": "Starting...",
                "last_update": time.time(),
                "metrics": {
                    "successful": 0,
                    "failed": 0,
                    "skipped": 0,
                    "urls_per_second": 0.0,
                    "memory_usage": 0,
                    "elapsed_time": 0,
                    "estimated_time_remaining": 0,
                    "content_stats": {}
                }
            }
    
    def update_from_status(self, status: Dict):
        """Update progress from status response"""
        # Update basic progress info
        progress = status.get("progress", 0)
        status_text = status.get("status", "In progress...")
        task_type = status.get("type")
        current_url = status.get("current_url")
        
        # Handle metrics based on task type
        if task_type == "discovery":
            metrics = {
                "successful": status.get("total_urls", 0),
                "failed": 0,
                "urls_per_second": 0.0,
                "current_url": current_url
            }
        else:
            metrics = status.get("metrics", {})
        
        # Update session state
        state_key = f"task_{self.task_type}_progress"
        st.session_state[state_key] = {
            "progress": progress,
            "status": status_text,
            "last_update": time.time(),
            "metrics": metrics
        }
        
        # Update UI
        self.update_progress(progress, status_text)
        self.update_metrics(metrics)
    
    def update_progress(self, progress: float, status: str):
        """Update progress bar and status"""
        with self.progress_container:
            self.progress_bar.progress(progress)
            self.status_text.write(f"📊 **Status:** {status}")
            
            # Show current URL if available
            current_url = st.session_state.get(f"task_{self.task_type}_progress", {}).get("metrics", {}).get("current_url")
            if current_url:
                st.write(f"🔗 Processing: `{current_url}`")
            
            # Show phase message
            if progress < 1.0:
                if self.task_type == "discovery":
                    st.info("🔍 Discovering URLs... Please wait...")
                else:
                    st.info("🌐 Crawling URLs in progress...")
            elif status.lower() == "completed":
                st.success("✅ Task completed!")
            elif status.lower() == "failed":
                st.error("❌ Task failed!")
    
    def update_metrics(self, metrics: Dict):
        """Update metrics display"""
        with self.progress_container:
            # Basic metrics
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                st.metric(
                    "✅ Successful",
                    metrics.get("successful", 0),
                    delta=self._get_delta("successful")
                )
            
            with col2:
                st.metric(
                    "❌ Failed",
                    metrics.get("failed", 0),
                    delta=self._get_delta("failed"),
                    delta_color="inverse"
                )
            
            with col3:
                urls_per_sec = metrics.get("urls_per_second", 0)
                st.metric(
                    "⚡ URLs/sec",
                    f"{urls_per_sec:.1f}",
                    delta=None
                )
            
            with col4:
                elapsed = metrics.get("elapsed_time", 0)
                eta = metrics.get("estimated_time_remaining", 0)
                if elapsed > 0:
                    st.metric(
                        "⏱️ Time",
                        f"{elapsed:.0f}s",
                        delta=f"{eta:.0f}s remaining" if eta > 0 else None
                    )
            
            # Content metrics if available
            content_stats = metrics.get("content_stats", {})
            if content_stats:
                st.subheader("📦 Content Stats")
                cols = st.columns(4)
                
                with cols[0]:
                    st.metric(
                        "📝 Text",
                        content_stats.get("text", 0)
                    )
                
                with cols[1]:
                    st.metric(
                        "🖼️ Images",
                        content_stats.get("images", 0)
                    )
                
                with cols[2]:
                    st.metric(
                        "📄 PDFs",
                        content_stats.get("pdfs", 0)
                    )
                
                with cols[3]:
                    st.metric(
                        "🔗 Links",
                        content_stats.get("links", 0)
                    )
    
    def _get_delta(self, metric_key: str) -> Optional[int]:
        """Calculate delta for a metric"""
        try:
            current = st.session_state[f"task_{self.task_type}_progress"]["metrics"][metric_key]
            previous = getattr(self, f"_last_{metric_key}", 0)
            setattr(self, f"_last_{metric_key}", current)
            return current - previous if previous > 0 else None
        except (KeyError, AttributeError):
            return None
    
    def get_current_progress(self) -> float:
        """Get current progress value"""
        try:
            return st.session_state[f"task_{self.task_type}_progress"]["progress"]
        except (KeyError, TypeError):
            return 0.0
    
    def get_current_status(self) -> str:
        """Get current status text"""
        try:
            return st.session_state[f"task_{self.task_type}_progress"]["status"]
        except (KeyError, TypeError):
            return "Unknown"
    
    def is_complete(self) -> bool:
        """Check if task is complete"""
        try:
            state = st.session_state[f"task_{self.task_type}_progress"]
            return (
                state["progress"] >= 1.0 or
                state["status"].lower() in ["completed", "failed"]
            )
        except (KeyError, TypeError):
            return False
    
    def cleanup(self):
        """Clean up session state and UI"""
        if f"task_{self.task_type}_progress" in st.session_state:
            del st.session_state[f"task_{self.task_type}_progress"]
        
        # Clear UI containers
        with self.progress_container:
            self.progress_bar.empty()
            self.status_text.empty()
            self.metrics_container.empty()
            self.content_metrics.empty()