from typing import Dict, List, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import streamlit as st

@dataclass
class CrawlerState:
    """Represents the current state of the crawler"""
    is_crawling: bool = False
    urls_discovered: List[str] = None
    urls_processed: List[str] = None
    current_batch: int = 0
    total_batches: int = 0
    start_time: Optional[datetime] = None
    last_update: Optional[datetime] = None
    
    def __post_init__(self):
        self.urls_discovered = self.urls_discovered or []
        self.urls_processed = self.urls_processed or []

class StateManager:
    """Manages application state in Streamlit's session state"""
    
    def __init__(self):
        """Initialize state if not already present"""
        if 'crawler_state' not in st.session_state:
            st.session_state.crawler_state = CrawlerState()
        
        if 'settings' not in st.session_state:
            st.session_state.settings = {}
        
        if 'error_log' not in st.session_state:
            st.session_state.error_log = []
    
    @property
    def crawler_state(self) -> CrawlerState:
        return st.session_state.crawler_state
    
    @crawler_state.setter
    def crawler_state(self, state: CrawlerState):
        st.session_state.crawler_state = state
    
    def update_crawler_state(self, **kwargs):
        """Update specific fields in crawler state"""
        current_state = asdict(self.crawler_state)
        current_state.update(kwargs)
        self.crawler_state = CrawlerState(**current_state)
    
    def start_crawling(self, discovered_urls: List[str]):
        """Initialize crawling state"""
        self.crawler_state = CrawlerState(
            is_crawling=True,
            urls_discovered=discovered_urls,
            start_time=datetime.now()
        )
    
    def stop_crawling(self):
        """Stop crawling and reset state"""
        self.crawler_state = CrawlerState(
            is_crawling=False,
            urls_discovered=self.crawler_state.urls_discovered,
            urls_processed=self.crawler_state.urls_processed
        )
    
    def update_progress(self, processed_urls: List[str], current_batch: int, total_batches: int):
        """Update crawling progress"""
        self.update_crawler_state(
            urls_processed=processed_urls,
            current_batch=current_batch,
            total_batches=total_batches,
            last_update=datetime.now()
        )
    
    def save_settings(self, settings: Dict):
        """Save current settings"""
        st.session_state.settings = settings
    
    def get_settings(self) -> Dict:
        """Get saved settings"""
        return st.session_state.settings
    
    def log_error(self, error: str, url: Optional[str] = None):
        """Add error to error log"""
        st.session_state.error_log.append({
            'timestamp': datetime.now(),
            'error': error,
            'url': url
        })
    
    def clear_error_log(self):
        """Clear error log"""
        st.session_state.error_log = []
    
    def get_error_log(self) -> List[Dict]:
        """Get current error log"""
        return st.session_state.error_log