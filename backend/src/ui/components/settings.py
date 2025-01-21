import streamlit as st
from typing import Dict, Any
from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode

def get_performance_settings(mode: str) -> Dict[str, Any]:
    """Get performance settings based on mode"""
    return {
        "Low": {
            "max_concurrent": 3,
            "batch_size": 5,
            "delay": 2.0,
            "memory_threshold": 1000
        },
        "Medium": {
            "max_concurrent": 5,
            "batch_size": 10,
            "delay": 1.0,
            "memory_threshold": 2000
        },
        "High": {
            "max_concurrent": 10,
            "batch_size": 20,
            "delay": 0.5,
            "memory_threshold": 4000
        },
        "Extreme": {
            "max_concurrent": 20,
            "batch_size": 50,
            "delay": 0.1,
            "memory_threshold": 8000
        }
    }.get(mode, {})

def render_crawler_settings() -> Dict[str, Any]:
    """Render crawler settings UI and return configuration"""
    
    st.sidebar.header("Crawler Settings")
    
    # Basic Settings
    st.sidebar.subheader("Basic Settings")
    browser_type = st.sidebar.selectbox(
        "Browser Type",
        ["chromium", "firefox", "webkit"],
        help="Select browser engine for crawling"
    )
    
    # Performance Settings
    st.sidebar.subheader("Performance Settings")
    performance_mode = st.sidebar.select_slider(
        "Performance Mode",
        options=["Low", "Medium", "High", "Extreme"],
        value="Medium",
        help="Higher modes use more system resources"
    )
    
    perf_settings = get_performance_settings(performance_mode)
    
    # Allow override of performance settings
    show_advanced = st.sidebar.checkbox("Show Advanced Settings")
    
    if show_advanced:
        st.sidebar.markdown("**Advanced Performance Settings**")
        perf_settings["max_concurrent"] = st.sidebar.slider(
            "Max Concurrent Requests",
            1, 50, perf_settings["max_concurrent"]
        )
        perf_settings["batch_size"] = st.sidebar.slider(
            "Batch Size",
            1, 100, perf_settings["batch_size"]
        )
        perf_settings["delay"] = st.sidebar.slider(
            "Delay Between Requests (seconds)",
            0.1, 5.0, perf_settings["delay"]
        )
        perf_settings["memory_threshold"] = st.sidebar.slider(
            "Memory Threshold (MB)",
            500, 16000, perf_settings["memory_threshold"]
        )
    
    # URL Filtering
    st.sidebar.subheader("URL Filtering")
    exclusion_patterns = st.sidebar.text_area(
        "Exclusion Patterns (one per line)",
        help="URLs matching these patterns will be skipped"
    ).split('\n')
    max_depth = st.sidebar.number_input(
        "Max Crawl Depth",
        min_value=1,
        max_value=10,
        value=3,
        help="Maximum depth for URL discovery"
    )
    
    # Anti-Bot Settings
    st.sidebar.subheader("Anti-Bot Protection")
    magic_mode = st.sidebar.checkbox(
        "Enable Magic Mode",
        value=True,
        help="Enhanced anti-bot protection"
    )
    simulate_user = st.sidebar.checkbox(
        "Simulate User Behavior",
        value=True,
        help="Mimic human-like browsing patterns"
    )
    
    # Media Settings
    st.sidebar.subheader("Media Handling")
    save_images = st.sidebar.checkbox(
        "Save Images",
        value=True,
        help="Download and save images from pages"
    )
    capture_screenshots = st.sidebar.checkbox(
        "Capture Screenshots",
        value=True,
        help="Save screenshots of crawled pages"
    )
    generate_pdfs = st.sidebar.checkbox(
        "Generate PDFs",
        value=True,
        help="Save PDFs of crawled pages"
    )
    
    # Create configurations
    browser_config = BrowserConfig(
        browser_type=browser_type,
        headless=True,
        user_agent_mode="random",
        viewport_width=1080,
        viewport_height=800
    )
    
    crawler_config = CrawlerRunConfig(
        magic=magic_mode,
        simulate_user=simulate_user,
        cache_mode=CacheMode.ENABLED,
        mean_delay=perf_settings["delay"],
        max_range=0.3,
        semaphore_count=perf_settings["max_concurrent"],
        
        # Media handling settings
        screenshot=capture_screenshots,
        pdf=generate_pdfs,
        exclude_external_images=not save_images,
        image_score_threshold=0,  # Accept all images
        
        # Media download settings
        scan_full_page=True,  # Ensure all images are found
        wait_for_images=save_images,  # Wait for images if saving enabled
        delay_before_return_html=2.0 if save_images else 0.5  # Extra time for loading
    )
    
    return {
        "browser_config": browser_config,
        "crawler_config": crawler_config,
        "batch_size": perf_settings["batch_size"],
        "memory_threshold": perf_settings["memory_threshold"],
        "max_depth": max_depth,
        "exclusion_patterns": [p for p in exclusion_patterns if p.strip()],
        "performance_mode": performance_mode
    }