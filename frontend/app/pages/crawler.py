import streamlit as st
import asyncio
import time
from components.crawler.url_input import URLInput
from components.crawler.crawler_settings import CrawlerSettings
from components.crawler.progress_monitor import ProgressMonitor
from components.crawler.results_viewer import ResultsViewer

async def discover_urls(url: str, settings: dict, progress: ProgressMonitor, viewer: ResultsViewer):
    """Run URL discovery process"""
    api_client = st.session_state.api_client
    ws_client = st.session_state.ws_client
    
    try:
        # Start URL discovery
        with st.spinner("🔍 Starting URL discovery..."):
            discovery = await api_client.discover_urls(url, "full", settings)
            task_id = discovery["task_id"]
            st.success("URL discovery started!")
        
        # Configure WebSocket callback
        progress_update_event = asyncio.Event()
        
        async def handle_progress(data):
            progress.update_progress(data)
            if data.get("progress") >= 1.0 or data.get("status") in ["completed", "failed"]:
                progress_update_event.set()
        
        # Start WebSocket connection for progress
        st.info("🔌 Connecting to WebSocket for updates...")
        connection_task = asyncio.create_task(
            ws_client.connect_progress(task_id, handle_progress)
        )
        
        # Wait for initial connection
        await asyncio.sleep(1)
        
        # Check WebSocket status
        if not ws_client.get_connection_status("progress")["connected"]:
            st.warning("⚠️ WebSocket connection not established. Updates may be delayed.")
        
        # Wait for discovery to complete with timeout
        try:
            await asyncio.wait_for(progress_update_event.wait(), timeout=30)
        except asyncio.TimeoutError:
            st.warning("⏳ URL discovery is taking longer than expected. You can view results when available.")
        
        # Get discovery results
        discovery_results = await api_client.get_discovery_results(task_id)
        
        if discovery_results:
            # Show discovery results
            viewer.show_discovery_results(discovery_results)
            
            # Check if we have URLs to crawl
            urls = discovery_results.get("discovered_urls", [])
            if urls:
                st.session_state.discovered_urls = urls
                st.session_state.discovery_task_id = task_id
                st.session_state.crawler_settings = settings
                st.experimental_rerun()
            else:
                st.warning("No URLs were discovered!")
        else:
            st.error("Failed to get discovery results!")
        
        return discovery_results
        
    except Exception as e:
        st.error(f"Error during URL discovery: {str(e)}")
        return None
    finally:
        await ws_client.disconnect("progress")

async def start_crawling(urls: list, settings: dict, progress: ProgressMonitor, viewer: ResultsViewer):
    """Start crawling process"""
    api_client = st.session_state.api_client
    ws_client = st.session_state.ws_client
    
    try:
        # Start crawling task
        with st.spinner("🚀 Starting crawler..."):
            crawl_task = await api_client.start_crawling(urls, settings)
            task_id = crawl_task["task_id"]
            st.success("Crawling started!")
        
        # Configure WebSocket callbacks
        progress_update_event = asyncio.Event()
        
        async def handle_progress(data):
            progress.update_progress(data)
            if data.get("progress") >= 1.0 or data.get("status") in ["completed", "failed"]:
                progress_update_event.set()
        
        async def handle_metrics(data):
            progress.update_metrics(data)
        
        # Start WebSocket connections
        st.info("🔌 Connecting to WebSockets for updates...")
        progress_task = asyncio.create_task(
            ws_client.connect_progress(task_id, handle_progress)
        )
        metrics_task = asyncio.create_task(
            ws_client.connect_metrics(task_id, handle_metrics)
        )
        
        # Wait for initial connections
        await asyncio.sleep(1)
        
        # Check WebSocket status
        progress_status = ws_client.get_connection_status("progress")
        metrics_status = ws_client.get_connection_status("metrics")
        
        if not progress_status["connected"] or not metrics_status["connected"]:
            st.warning("⚠️ Some WebSocket connections failed. Updates may be delayed.")
        
        # Wait for crawling to complete with periodic updates
        while not progress_update_event.is_set():
            await asyncio.sleep(1)
            progress.show_connection_status()
            
            # Show intermediate results if available
            for url in urls:
                try:
                    results = await api_client.get_results(url)
                    files = await api_client.get_files(url)
                    if results and results["success"]:
                        viewer.show_crawl_results(url, results, files)
                except Exception:
                    pass
        
        # Show final results
        if progress.get_current_status() == "completed":
            st.success("🎉 Crawling completed successfully!")
            
            for url in urls:
                results = await api_client.get_results(url)
                files = await api_client.get_files(url)
                viewer.show_crawl_results(url, results, files)
        else:
            st.error("❌ Crawling failed or was interrupted!")
        
    except Exception as e:
        st.error(f"Error during crawling: {str(e)}")
    finally:
        await ws_client.disconnect()

def show():
    """Show crawler page"""
    st.header("🌐 Web Crawler")
    
    # Initialize components
    progress = None
    viewer = ResultsViewer(None)
    
    # Main UI
    tab1, tab2 = st.tabs(["🎯 URL Input & Settings", "📊 Results"])
    
    with tab1:
        # URL input
        url, is_valid = URLInput.render()
        
        # Crawler settings
        settings = CrawlerSettings.render()
        
        # Discovery button
        if url and is_valid and "discovered_urls" not in st.session_state:
            if st.button("🔍 Discover URLs", type="primary"):
                progress = ProgressMonitor("discovery")
                asyncio.run(discover_urls(url, settings, progress, viewer))
        
        # Show crawl button if URLs discovered
        if "discovered_urls" in st.session_state:
            urls = st.session_state.discovered_urls
            if urls:
                st.success(f"🎯 Discovered {len(urls)} URLs!")
                if st.button("🚀 Start Crawling", type="primary"):
                    progress = ProgressMonitor("crawl")
                    asyncio.run(start_crawling(urls, settings, progress, viewer))
    
    with tab2:
        if "discovery_task_id" in st.session_state:
            st.subheader("🔍 Discovery Results")
            viewer = ResultsViewer(st.session_state.discovery_task_id)
            
            # Reload results button
            if st.button("🔄 Reload Results"):
                st.experimental_rerun()
        else:
            st.info("Start URL discovery to see results here!")
    
    # Show WebSocket status if active
    if progress:
        progress.show_connection_status()
