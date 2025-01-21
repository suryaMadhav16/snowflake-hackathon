import streamlit as st
import asyncio
import time
from components.crawler.url_input import URLInput
from components.crawler.crawler_settings import CrawlerSettings
from components.crawler.progress_monitor import ProgressMonitor
from components.crawler.results_viewer import ResultsViewer
import logging

logger = logging.getLogger(__name__)

async def discover_urls(url: str, settings: dict, progress: ProgressMonitor) -> dict:
    """Run URL discovery process"""
    api_client = st.session_state.api_client
    
    try:
        # Start URL discovery
        with st.spinner("🔍 Starting URL discovery..."):
            # Handle status updates via polling
            async def handle_status(status: dict):
                progress.update_from_status(status)
                logger.debug(f"Received status update: {status}")
            
            discovery = await api_client.discover_urls(url, "full", settings, on_update=handle_status)
            task_id = discovery["task_id"]
            st.success("URL discovery started!")
            
            # Wait for discovery to complete or timeout
            start_time = time.time()
            timeout = 60  # 1 minute timeout
            
            while not progress.is_complete() and (time.time() - start_time) < timeout:
                try:
                    status = await api_client.get_status(task_id)
                    progress.update_from_status(status)
                    if progress.is_complete():
                        break
                except Exception as e:
                    logger.error(f"Error checking status: {str(e)}")
                await asyncio.sleep(1)
            
            if not progress.is_complete():
                st.warning("⏳ URL discovery is taking longer than expected...")
        
        # Get discovery results
        discovery_results = await api_client.get_discovery_results(discovery["task_id"])
        
        if discovery_results and "discovered_urls" in discovery_results:
            urls = discovery_results["discovered_urls"]
            if urls:
                st.session_state["discovered_urls"] = urls
                st.session_state["discovery_task_id"] = discovery["task_id"]
                st.session_state["discovery_settings"] = settings
                st.success(f"🎯 Discovered {len(urls)} URLs!")
                return discovery_results
        
        st.warning("No URLs were discovered!")
        return None
        
    except Exception as e:
        st.error(f"Error during URL discovery: {str(e)}")
        return None

async def start_crawling(urls: list, settings: dict, progress: ProgressMonitor, viewer: ResultsViewer):
    """Start crawling process"""
    api_client = st.session_state.api_client
    
    try:
        # Start crawling
        with st.spinner("🚀 Starting crawler..."):
            # Handle status updates
            async def handle_status(status: dict):
                progress.update_from_status(status)
                logger.debug(f"Received crawl status update: {status}")
                
                # If we have results, update the viewer
                if status.get("type") == "crawl" and status.get("status") == "completed":
                    current_url = status.get("current_url")
                    if current_url:
                        results = await api_client.get_results(current_url)
                        files = await api_client.get_files(current_url)
                        if results:
                            viewer.show_crawl_results(current_url, results, files)
            
            crawl_task = await api_client.start_crawling(urls, settings, on_update=handle_status)
            task_id = crawl_task["task_id"]
            st.success("Crawling started!")
            
            # Wait for crawling to complete or timeout
            start_time = time.time()
            timeout = 300  # 5 minutes timeout
            
            while not progress.is_complete() and (time.time() - start_time) < timeout:
                try:
                    status = await api_client.get_status(task_id)
                    progress.update_from_status(status)
                    if progress.is_complete():
                        break
                except Exception as e:
                    logger.error(f"Error checking crawl status: {str(e)}")
                await asyncio.sleep(1)
            
            if progress.is_complete():
                # Show final results for all URLs
                if progress.get_current_status() == "completed":
                    st.success("🎉 Crawling completed successfully!")
                    for url in urls:
                        try:
                            results = await api_client.get_results(url)
                            files = await api_client.get_files(url)
                            if results:
                                viewer.show_crawl_results(url, results, files)
                        except Exception as e:
                            st.error(f"Error getting results for {url}: {str(e)}")
                else:
                    st.error("❌ Crawling failed or was interrupted!")
            else:
                st.warning("⏳ Crawling is taking longer than expected...")
            
    except Exception as e:
        st.error(f"Error during crawling: {str(e)}")

def show():
    """Show crawler page"""
    st.title("🌐 Web Crawler")
    
    # Initialize components
    progress = None
    viewer = ResultsViewer(st.session_state.get("discovery_task_id"))
    
    # Main UI
    tab1, tab2 = st.tabs(["🎯 URL Input & Settings", "📊 Results"])
    
    with tab1:
        # URL input
        url, is_valid = URLInput.render()
        
        # Crawler settings
        settings = CrawlerSettings.render()
        
        # Discovery section
        if url and is_valid:
            if "discovered_urls" not in st.session_state:
                # Show discovery button
                if st.button("🔍 Discover URLs", type="primary", key="discover_btn"):
                    progress = ProgressMonitor("discovery")
                    asyncio.run(discover_urls(url, settings, progress))
            else:
                # Show discovered URLs count and crawl button
                urls = st.session_state["discovered_urls"]
                st.success(f"🎯 Discovered {len(urls)} URLs")
                
                # Option to clear discovery and start over
                if st.button("🔄 New Discovery", key="new_discovery_btn"):
                    for key in ["discovered_urls", "discovery_task_id", "discovery_settings"]:
                        if key in st.session_state:
                            del st.session_state[key]
                    st.rerun()
                
                # Start crawling button
                if st.button("🚀 Start Crawling", type="primary", key="crawl_btn"):
                    progress = ProgressMonitor("crawl")
                    asyncio.run(start_crawling(urls, settings, progress, viewer))
    
    with tab2:
        if "discovery_task_id" in st.session_state:
            st.subheader("🔍 Discovery Results")
            viewer = ResultsViewer(st.session_state["discovery_task_id"])
            
            # Option to refresh results
            if st.button("🔄 Refresh Results", key="refresh_btn"):
                st.rerun()
        else:
            st.info("Start URL discovery to see results here!")