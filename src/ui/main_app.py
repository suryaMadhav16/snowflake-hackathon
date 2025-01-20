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
        if 'crawl_mode' not in st.session_state:
            st.session_state.crawl_mode = 'full'
    
    async def discover_urls(self, url: str, settings: Dict, mode: str = 'full') -> List[str]:
        """Discover URLs based on selected mode"""
        url_manager = URLManager(settings['crawler_config'])
        st.session_state.is_discovering = True
        
        try:
            status_message = "Discovering URLs..." if mode == 'full' else "Validating URL..."
            with st.spinner(status_message):
                if mode == 'full':
                    logger.info(f"Starting full site crawl for {url}")
                    urls = await url_manager.discover_urls(
                        url,
                        max_depth=settings['max_depth']
                    )
                else:
                    logger.info(f"Starting single page validation for {url}")
                    urls = await url_manager.discover_single_url(url)
                    
                st.session_state.discovered_urls = urls
                st.session_state.url_graph = url_manager.url_graph
                return urls
                
        except Exception as e:
            error_type = "discovery" if mode == 'full' else "validation"
            st.error(f"Error during URL {error_type}: {str(e)}")
            logger.error(f"URL {error_type} error: {str(e)}", exc_info=True)
            return []
            
        finally:
            st.session_state.is_discovering = False
    
    async def process_urls(self, urls: List[str], settings: Dict):
        """Process discovered URLs in batches"""
        st.session_state.is_processing = True
        mode = st.session_state.crawl_mode
        
        try:
            crawler = BatchCrawler(
                browser_config=settings['browser_config'],
                crawl_config=settings['crawler_config']
            )
            
            total_urls = len(urls)
            processed = 0
            batch_size = 1 if mode == 'single' else settings['batch_size']
            
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            action = "Scraping" if mode == 'single' else "Processing"
            
            async for batch_results in crawler.process_batch(urls, batch_size):
                # Update progress
                processed += len(batch_results)
                progress = processed / total_urls
                progress_bar.progress(progress)
                
                # Update status
                status_text.text(
                    f"{action} page..." if mode == 'single' 
                    else f"Processed {processed}/{total_urls} URLs"
                )
                
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
            action = "scraping" if mode == 'single' else "crawling"
            st.error(f"Error during {action}: {str(e)}")
            logger.error(f"{action.capitalize()} error: {str(e)}", exc_info=True)
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
        
        # URL input and crawl mode selection
        url = st.text_input("Enter website URL")
        
        mode = st.radio(
            "Select Crawl Mode",
            ["Full Website Crawl", "Single Page"],
            help=("Full Website Crawl will discover and process all pages. "
                  "Single Page will only process the given URL.")
        )
        
        st.session_state.crawl_mode = 'full' if mode == "Full Website Crawl" else 'single'
        
        # Create two columns for main content
        col1, col2 = st.columns([2, 1])
        
        with col1:
            # URL Discovery/Validation Phase
            if url and not st.session_state.discovered_urls:
                action = "Crawl Site" if st.session_state.crawl_mode == 'full' else "Scrape Page"
                start_button = st.button(
                    action,
                    disabled=st.session_state.is_discovering,
                    key='start_discovery_button'
                )
                
                if start_button:
                    self.monitor.clear_metrics()
                    self.results.clear_results()
                    
                    discovered_urls = await self.discover_urls(
                        url, 
                        settings,
                        mode=st.session_state.crawl_mode
                    )
                    
                    if discovered_urls:
                        if st.session_state.crawl_mode == 'full':
                            st.success(f"Discovered {len(discovered_urls)} URLs")
                            # Show URL tree for full crawl
                            self.url_tree.render_url_tree(
                                discovered_urls, 
                                st.session_state.url_graph
                            )
                        else:
                            st.success("URL validated successfully")
                    else:
                        st.warning(
                            "No URLs discovered. Please check the URL and try again."
                            if st.session_state.crawl_mode == 'full'
                            else "Invalid URL. Please check and try again."
                        )
            
            # URL Tree Display (after discovery, only for full mode)
            elif (
                st.session_state.discovered_urls and 
                not st.session_state.is_processing and 
                st.session_state.crawl_mode == 'full'
            ):
                self.url_tree.render_url_tree(
                    st.session_state.discovered_urls,
                    st.session_state.url_graph
                )
        
        with col2:
            # Processing Phase
            if st.session_state.discovered_urls:
                st.subheader("Processing Controls")
                
                if not st.session_state.is_processing:
                    action = "Start Processing" if st.session_state.crawl_mode == 'full' else "Scrape Page"
                    if st.button(action, key='start_processing_button'):
                        self.state.start_crawling(st.session_state.discovered_urls)
                        await self.process_urls(
                            st.session_state.discovered_urls,
                            settings
                        )
                        
                    if st.button("Clear and Start Over", key='clear_button'):
                        st.session_state.discovered_urls = None
                        st.session_state.url_graph = None
                        st.rerun()
                else:
                    status = "Processing" if st.session_state.crawl_mode == 'full' else "Scraping"
                    st.info(f"{status} in progress...")
        
        # Show error log if exists
        errors = self.state.get_error_log()
        if errors:
            with st.expander("Error Log", expanded=False):
                for error in errors:
                    st.error(
                        f"{error['timestamp'].strftime('%H:%M:%S')}: {error['error']}"
                        + (f" ({error['url']})" if error['url'] else "")
                    )
                if st.button("Clear Error Log", key='clear_error_log_button'):
                    self.state.clear_error_log()

def main():
    """Entry point for Streamlit application"""
    app = CrawlerApp()
    asyncio.run(app.run())