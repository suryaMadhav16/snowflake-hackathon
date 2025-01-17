import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import asyncio
from urllib.parse import urlparse
import logging
from datetime import datetime
import json
from scraper import WebScraper, CrawlerMode

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(
    page_title="Web Scraper Dashboard",
    layout="wide",
    menu_items={
        'About': "Web Crawler powered by Crawl4AI"
    }
)

# Initialize session state
if 'crawling' not in st.session_state:
    st.session_state.crawling = False
if 'stats' not in st.session_state:
    st.session_state.stats = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'memory_usage' not in st.session_state:
    st.session_state.memory_usage = []
if 'performance_metrics' not in st.session_state:
    st.session_state.performance_metrics = []

async def update_stats(url: str, output_dir: str):
    """Update stats periodically"""
    while st.session_state.crawling:
        domain = urlparse(url).netloc
        db_path = Path(output_dir) / domain / 'stats.db'
        
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            
            # Get comprehensive stats
            stats = pd.read_sql_query('''
                SELECT 
                    cs.*,
                    COUNT(DISTINCT p.id) as pages_count,
                    COUNT(DISTINCT i.id) as images_count,
                    COUNT(DISTINCT l.id) as links_count,
                    COUNT(DISTINCT CASE WHEN p.pdf_path IS NOT NULL AND p.pdf_path != '' THEN p.id END) as pdfs_count,
                    COUNT(DISTINCT CASE WHEN p.screenshot_path IS NOT NULL AND p.screenshot_path != '' THEN p.id END) as screenshots_count
                FROM crawl_stats cs
                LEFT JOIN pages p ON p.crawled_at BETWEEN cs.start_time AND COALESCE(cs.end_time, datetime('now'))
                LEFT JOIN images i ON i.page_id = p.id and i.filepath IS NOT NULL
                LEFT JOIN links l ON l.page_id = p.id
                WHERE cs.id = (SELECT MAX(id) FROM crawl_stats)
                GROUP BY cs.id
            ''', conn)
            
            if not stats.empty:
                # Update stats in session state
                st.session_state.stats = {
                    'total_urls': stats['total_urls'].iloc[0],
                    'successful': stats['successful'].iloc[0],
                    'failed': stats['failed'].iloc[0],
                    'current_memory_usage': stats['current_memory_usage'].iloc[0],
                    'pages_count': stats['pages_count'].iloc[0],
                    'images_count': stats['images_count'].iloc[0],
                    'links_count': stats['links_count'].iloc[0],
                    'pdfs_count': stats['pdfs_count'].iloc[0],
                    'screenshots_count': stats['screenshots_count'].iloc[0]
                }
                
                # Update memory usage history
                st.session_state.memory_usage.append({
                    'time': datetime.now().strftime('%H:%M:%S'),
                    'usage': stats['current_memory_usage'].iloc[0]
                })
                
                if len(st.session_state.memory_usage) > 50:
                    st.session_state.memory_usage.pop(0)
                
            conn.close()
            st.session_state.last_update = datetime.now()
        
        await asyncio.sleep(2)

async def start_crawling(url: str, output_dir: str, exclusion_patterns: str,
                        browser_type: str = "chromium", test_mode: bool = False,
                        max_concurrent: int = 50, requests_per_second: float = 10.0,
                        memory_threshold_mb: int = 4000, batch_size: int = 200,
                        advanced_config: dict = None):
    """Start the crawling process with enhanced parallel processing"""
    try:
        # Process exclusion patterns
        patterns = [p.strip() for p in exclusion_patterns.split(',') if p.strip()]
        
        # Initialize scraper with configuration
        scraper = WebScraper(
            base_url=url,
            output_dir=output_dir,
            exclusion_patterns=patterns,
            max_concurrent=max_concurrent,
            requests_per_second=requests_per_second,
            memory_threshold_mb=memory_threshold_mb,
            batch_size=batch_size,
            test_mode=test_mode,
            browser_type=browser_type,
            enable_screenshots=advanced_config.get('screenshot', False),  # Default to False
            enable_pdfs=advanced_config.get('pdf', False),               # Default to False
            enable_magic=advanced_config.get('magic', True),
            simulate_user=advanced_config.get('simulate_user', True)
        )
        
        # Start stats update task
        st.session_state.crawling = True
        stats_task = asyncio.create_task(update_stats(url, output_dir))
        
        # Start crawling
        await scraper.crawl()
        
        # Clean up
        st.session_state.crawling = False
        await stats_task
        scraper.close()
        
    except Exception as e:
        st.error(f"Error during crawling: {str(e)}")
        logger.error(f"Crawling error: {str(e)}", exc_info=True)
        st.session_state.crawling = False

def main():
    st.title("Web Scraper Dashboard")
    
    # Sidebar configuration
    with st.sidebar:
        st.header("Configuration")
        
        # Basic Settings
        st.subheader("Basic Settings")
        browser_type = st.selectbox(
            "Browser Type",
            ["chromium", "firefox", "webkit"],
            help="Select browser engine for crawling"
        )
        
        # Performance mode
        performance_mode = st.select_slider(
            "Performance Mode",
            options=["Low", "Medium", "High", "Extreme"],
            value="Medium",
            help="Select crawler performance mode. Higher modes use more system resources."
        )
        
        if performance_mode in ["High", "Extreme"]:
            st.warning("""
            ⚠️ High/Extreme modes consume significant system resources.
            Monitor system performance and target server responses.
            """)
        
        mode_settings = CrawlerMode.get_mode(performance_mode.lower())
        max_concurrent = mode_settings['max_concurrent']
        requests_per_second = mode_settings['requests_per_second']
        memory_threshold = mode_settings['memory_threshold_mb']
        batch_size = mode_settings['batch_size']
        
        # Advanced Settings
        st.subheader("Advanced Settings")
        show_advanced = st.checkbox("Show Advanced Settings")
        
        advanced_config = {
            'screenshot': False,  # Default to False
            'pdf': False,        # Default to False
            'magic': True,
            'simulate_user': True
        }
        
        if show_advanced:
            st.info("Current Performance Settings:")
            st.code(f"""
            Max Concurrent: {max_concurrent}
            Requests/Second: {requests_per_second}
            Memory Limit: {memory_threshold} MB
            Batch Size: {batch_size}
            """)
            
            # Allow override of mode settings
            if st.checkbox("Override Performance Settings"):
                max_concurrent = st.slider(
                    "Max Concurrent Requests",
                    min_value=1,
                    max_value=100,
                    value=max_concurrent
                )
                
                requests_per_second = st.slider(
                    "Requests Per Second",
                    min_value=1.0,
                    max_value=20.0,
                    value=requests_per_second
                )
                
                memory_threshold = st.slider(
                    "Memory Threshold (MB)",
                    min_value=500,
                    max_value=8000,
                    value=memory_threshold,
                    step=500
                )
                
                batch_size = st.slider(
                    "Batch Size",
                    min_value=10,
                    max_value=500,
                    value=batch_size,
                    step=10
                )
            
            # Media settings
            st.subheader("Media Settings")
            advanced_config.update({
                'screenshot': st.checkbox("Capture Screenshots", value=False),
                'pdf': st.checkbox("Generate PDFs", value=False),
                'magic': st.checkbox("Anti-Bot Protection", value=True),
                'simulate_user': st.checkbox("Simulate User Behavior", value=True)
            })
    
    # Main content
    # Input form
    with st.form("crawl_form"):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            url = st.text_input("Enter website URL to crawl")
            exclusion_patterns = st.text_input(
                "Exclusion Patterns (comma-separated)", 
                help="Regex patterns for URLs to exclude (e.g., 'blog/\\d+,/tag/.*')"
            )
            output_dir = st.text_input("Output Directory", "/tmp/webscraper")
        
        with col2:
            test_mode = st.checkbox(
                "Test Mode (limit to 15 pages)", 
                value=True,
                help="Enable test mode to limit crawling to maximum 15 pages"
            )
            start_button = st.form_submit_button("Start Crawling")
    
    # Start crawling when button is pressed
    if start_button:
        try:
            # Validate URL
            result = urlparse(url)
            if not all([result.scheme, result.netloc]):
                st.error("Invalid URL format")
                return
            
            # Create output directory
            Path(output_dir).mkdir(parents=True, exist_ok=True)
            
            # Start crawling with configurations
            with st.spinner("Crawling in progress..."):
                asyncio.run(start_crawling(
                    url=url,
                    output_dir=output_dir,
                    exclusion_patterns=exclusion_patterns,
                    browser_type=browser_type,
                    test_mode=test_mode,
                    max_concurrent=max_concurrent,
                    requests_per_second=requests_per_second,
                    memory_threshold_mb=memory_threshold,
                    batch_size=batch_size,
                    advanced_config=advanced_config
                ))
            
        except Exception as e:
            st.error(f"Error: {str(e)}")
    
    # Display current status
    if st.session_state.stats:
        st.header("Crawling Status")
        
        # Display metrics in multiple rows
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total URLs", st.session_state.stats.get('total_urls', 0))
            st.metric("Pages Processed", st.session_state.stats.get('pages_count', 0))
        with col2:
            st.metric("Successful", st.session_state.stats.get('successful', 0))
            st.metric("Failed", st.session_state.stats.get('failed', 0))
        with col3:
            st.metric("Memory Usage (MB)", f"{st.session_state.stats.get('current_memory_usage', 0):.1f}")
            success_rate = (st.session_state.stats.get('successful', 0) / 
                          max(st.session_state.stats.get('total_urls', 1), 1) * 100)
            st.metric("Success Rate", f"{success_rate:.1f}%")
        
        # Media stats
        st.subheader("Media Statistics")
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Images", st.session_state.stats.get('images_count', 0))
        col2.metric("Links", st.session_state.stats.get('links_count', 0))
        col3.metric("PDFs", st.session_state.stats.get('pdfs_count', 0))
        col4.metric("Screenshots", st.session_state.stats.get('screenshots_count', 0))
        
        # Performance charts
        st.subheader("Performance Metrics")
        col1, col2 = st.columns(2)
        
        with col1:
            # Memory usage trend
            if st.session_state.memory_usage:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=[entry['time'] for entry in st.session_state.memory_usage],
                    y=[entry['usage'] for entry in st.session_state.memory_usage],
                    mode='lines+markers',
                    name='Memory Usage (MB)'
                ))
                fig.update_layout(
                    title="Memory Usage Trend",
                    xaxis_title="Time",
                    yaxis_title="Memory Usage (MB)",
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Success/Failure pie chart
            if st.session_state.stats.get('total_urls', 0) > 0:
                fig = go.Figure(data=[go.Pie(
                    labels=['Successful', 'Failed'],
                    values=[
                        st.session_state.stats.get('successful', 0),
                        st.session_state.stats.get('failed', 0)
                    ],
                    hole=.3
                )])
                fig.update_layout(
                    title="Crawling Results",
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)
        
        if st.session_state.last_update:
            st.caption(f"Last updated: {st.session_state.last_update.strftime('%H:%M:%S')}")

if __name__ == "__main__":
    main()