import streamlit as st
import asyncio
from components.crawler.url_input import URLInput
from components.crawler.crawler_settings import CrawlerSettings
from components.crawler.progress_monitor import ProgressMonitor
from components.crawler.results_viewer import ResultsViewer

async def discover_and_crawl(url: str, settings: dict):
    """Run URL discovery and crawling process"""
    api_client = st.session_state.api_client
    ws_client = st.session_state.ws_client
    
    try:
        # Start URL discovery
        discovery = await api_client.discover_urls(url, "full", settings)
        task_id = discovery["task_id"]
        
        # Monitor discovery progress
        progress = ProgressMonitor(task_id)
        
        # Connect WebSocket
        progress_update_event = asyncio.Event()
        
        async def handle_progress(data):
            progress.update_progress(data)
            if data.get("progress") >= 1.0 or data.get("status") in ["completed", "failed"]:
                progress_update_event.set()
        
        # Start WebSocket connection
        asyncio.create_task(
            ws_client.connect_progress(task_id, handle_progress)
        )
        
        # Wait for discovery to complete
        await progress_update_event.wait()
        
        if progress.get_current_status() == "failed":
            st.error("URL discovery failed")
            return
        
        # Get discovery results
        discovery_results = await api_client.get_discovery_results(task_id)
        if not discovery_results:
            st.error("No URLs discovered")
            return
        
        # Show discovery results
        viewer = ResultsViewer(task_id)
        viewer.show_discovery_results(discovery_results)
        
        # Start crawling if URLs were discovered
        urls = discovery_results.get("discovered_urls", [])
        if not urls:
            st.warning("No URLs to crawl")
            return
        
        # Confirm crawling
        if st.button("Start Crawling", type="primary"):
            # Start crawling task
            crawl_task = await api_client.start_crawling(urls, settings)
            task_id = crawl_task["task_id"]
            
            # Reset and reuse progress monitor
            progress.cleanup()
            progress = ProgressMonitor(task_id)
            
            # Reset event
            progress_update_event.clear()
            
            # Connect metrics WebSocket
            async def handle_metrics(data):
                progress.update_metrics(data)
            
            # Start WebSocket connections
            asyncio.create_task(
                ws_client.connect_progress(task_id, handle_progress)
            )
            asyncio.create_task(
                ws_client.connect_metrics(task_id, handle_metrics)
            )
            
            # Wait for crawling to complete
            await progress_update_event.wait()
            
            # Show final results
            if progress.get_current_status() == "completed":
                st.success("Crawling completed successfully!")
                
                # Show results for each URL
                for url in urls:
                    results = await api_client.get_results(url)
                    files = await api_client.get_files(url)
                    viewer.show_crawl_results(url, results, files)
    
    except Exception as e:
        st.error(f"Error: {str(e)}")
    finally:
        # Cleanup
        await ws_client.disconnect()

def show():
    """Show crawler page"""
    st.header("🌐 Web Crawler")
    
    # URL input
    url, is_valid = URLInput.render()
    
    # Crawler settings
    settings = CrawlerSettings.render()
    
    # Start button
    if url and is_valid:
        if st.button("Discover URLs", type="primary"):
            asyncio.run(discover_and_crawl(url, settings))
    else:
        st.info("Enter a valid URL to start crawling")
