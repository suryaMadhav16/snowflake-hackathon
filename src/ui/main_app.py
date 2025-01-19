import streamlit as st
import asyncio
import logging
from typing import Dict, List
from datetime import datetime
from urllib.parse import urlparse

from ..core.url_manager import URLManager
from ..core.crawler import BatchCrawler
from ..database.db_manager import DatabaseManager
from .components.settings import render_crawler_settings
from .components.monitor import CrawlerMonitor
from .components.results import ResultsDisplay
from .components.url_tree import URLTreeVisualizer
from .state import StateManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CrawlerApp:
    """Main Streamlit application for web crawler"""
    
    def __init__(self):
        """Initialize application components"""
        self.state = StateManager()
        self.monitor = CrawlerMonitor()
        self.results = ResultsDisplay()
        self.db = DatabaseManager()
        self.url_tree = URLTreeVisualizer()
        
        # Initialize session state variables
        if 'discovered_urls' not in st.session_state:
            st.session_state.discovered_urls = None
        if 'is_discovering' not in st.session_state:
            st.session_state.is_discovering = False
        if 'is_processing' not in st.session_state:
            st.session_state.is_processing = False
        if 'url_graph' not in st.session_state:
            st.session_state.url_graph = None
    
    async def discover_urls(self, url: str, settings: Dict) -> List[str]:
        """Discover URLs from starting point"""
        url_manager = URLManager(settings['crawler_config'])
        st.session_state.is_discovering = True
        
        try:
            with st.spinner("Discovering URLs..."):
                urls = await url_manager.discover_urls(
                    url,
                    max_depth=settings['max_depth']
                )
                st.session_state.discovered_urls = urls
                st.session_state.url_graph = url_manager.url_graph
                return urls
                
        except Exception as e:
            st.error(f"Error during URL discovery: {str(e)}")
            logger.error(f"URL discovery error: {str(e)}", exc_info=True)
            return []
            
        finally:
            st.session_state.is_discovering = False
    
    async def process_urls(self, urls: List[str], settings: Dict):
        """Process discovered URLs in batches"""
        st.session_state.is_processing = True
        
        try:
            crawler = BatchCrawler(
                browser_config=settings['browser_config'],
                crawl_config=settings['crawler_config']
            )
            
            total_urls = len(urls)
            processed = 0
            batch_size = settings['batch_size']
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            async for batch_results in crawler.process_batch(urls, batch_size):
                # Update progress
                processed += len(batch_results)
                progress = processed / total_urls
                progress_bar.progress(progress)
                
                # Update status
                status_text.text(f"Processed {processed}/{total_urls} URLs")
                
                # Update metrics and display
                current_metrics = crawler.get_metrics()
                self.monitor.update_metrics(current_metrics)
                self.monitor.render_progress(total_urls, processed, current_metrics)
                self.results.render_results_summary(batch_results)
                
                # Save to database
                await self.db.save_results(batch_results)
                
                # Update state
                self.state.update_progress(
                    processed_urls=[r.url for r in batch_results if r.success],
                    current_batch=current_metrics['current_batch'],
                    total_batches=current_metrics['total_batches']
                )
                
        except Exception as e:
            st.error(f"Error during crawling: {str(e)}")
            logger.error(f"Crawling error: {str(e)}", exc_info=True)
            self.state.log_error(str(e))
            
        finally:
            st.session_state.is_processing = False
            self.state.stop_crawling()
    
    async def run(self):
        """Main application flow"""
        st.title("Web Crawler")
        
        # Initialize database
        await self.db.initialize()
        
        # Render settings
        settings = render_crawler_settings()
        self.state.save_settings(settings)
        
        # URL input and validation
        url = st.text_input("Enter website URL to crawl")
        
        # Create two columns for main content
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # URL Discovery Phase
            if url and not st.session_state.discovered_urls:
                start_button = st.button(
                    "Start Crawling",
                    disabled=st.session_state.is_discovering
                )
                
                if start_button:
                    self.monitor.clear_metrics()
                    self.results.clear_results()
                    
                    discovered_urls = await self.discover_urls(url, settings)
                    
                    if discovered_urls:
                        st.success(f"Discovered {len(discovered_urls)} URLs")
                        
                        # Show URL tree
                        self.url_tree.render_url_tree(
                            discovered_urls, 
                            st.session_state.url_graph
                        )
                    else:
                        st.warning("No URLs discovered. Please check the URL and try again.")
            
            # URL Tree Display (after discovery)
            elif st.session_state.discovered_urls and not st.session_state.is_processing:
                self.url_tree.render_url_tree(
                    st.session_state.discovered_urls,
                    st.session_state.url_graph
                )
        
        with col2:
            # Processing Phase
            if st.session_state.discovered_urls:
                st.subheader("Processing Controls")
                
                if not st.session_state.is_processing:
                    if st.button("Start Processing"):
                        self.state.start_crawling(st.session_state.discovered_urls)
                        await self.process_urls(
                            st.session_state.discovered_urls,
                            settings
                        )
                        
                    if st.button("Clear and Start Over"):
                        st.session_state.discovered_urls = None
                        st.session_state.url_graph = None
                        st.rerun()
                else:
                    st.info("Processing in progress...")
        
        # Show error log if exists
        errors = self.state.get_error_log()
        if errors:
            with st.expander("Error Log", expanded=False):
                for error in errors:
                    st.error(
                        f"{error['timestamp'].strftime('%H:%M:%S')}: {error['error']}"
                        + (f" ({error['url']})" if error['url'] else "")
                    )
                if st.button("Clear Error Log"):
                    self.state.clear_error_log()

def main():
    """Entry point for Streamlit application"""
    app = CrawlerApp()
    asyncio.run(app.run())