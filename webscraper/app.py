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
from scraper import WebScraper
from utils.url_analyzer import URLPatternAnalyzer
from utils.history_analyzer import CrawlHistoryAnalyzer

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Web Scraper Dashboard", layout="wide")

# Initialize session state
if 'crawling' not in st.session_state:
    st.session_state.crawling = False
if 'stats' not in st.session_state:
    st.session_state.stats = None
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'memory_usage' not in st.session_state:
    st.session_state.memory_usage = []
if 'url_patterns' not in st.session_state:
    st.session_state.url_patterns = {}
if 'selected_patterns' not in st.session_state:
    st.session_state.selected_patterns = set()
if 'analyzer' not in st.session_state:
    st.session_state.analyzer = None

async def analyze_sitemap(url: str):
    """Analyze sitemap and update patterns"""
    analyzer = URLPatternAnalyzer(url)
    st.session_state.analyzer = analyzer  # Store analyzer for later use
    patterns = await analyzer.analyze_sitemap()
    st.session_state.url_patterns = patterns
    return patterns

def display_pattern_selection():
    """Display URL pattern selection interface"""
    st.subheader("Select URL Patterns to Crawl")
    
    if not st.session_state.url_patterns:
        st.info("No URL patterns found. Please analyze sitemap first.")
        return set()
    
    total_urls = 0
    selected_patterns = set()
    
    # Create a table for pattern display
    col1, col2, col3, col4 = st.columns([0.5, 2, 1, 2])
    with col1:
        st.markdown("**Select**")
    with col2:
        st.markdown("**Pattern**")
    with col3:
        st.markdown("**Count**")
    with col4:
        st.markdown("**Example URL**")
    
    for pattern, data in st.session_state.url_patterns.items():
        col1, col2, col3, col4 = st.columns([0.5, 2, 1, 2])
        with col1:
            if st.checkbox("", key=f"pattern_{hash(pattern)}"):
                selected_patterns.add(pattern)
                total_urls += data['count']
        with col2:
            st.text(pattern)
        with col3:
            st.text(data['count'])
        with col4:
            st.markdown(f"<small>{data['example']}</small>", unsafe_allow_html=True)
    
    if selected_patterns:
        st.info(f"Selected {len(selected_patterns)} patterns with total {total_urls} URLs")
    
    st.session_state.selected_patterns = selected_patterns
    return selected_patterns

def display_crawl_history():
    """Display crawl history for all domains"""
    st.header("Crawl History")
    
    analyzer = CrawlHistoryAnalyzer()
    stats = analyzer.get_all_stats()
    
    if not stats:
        st.info("No crawl history found. Start crawling to see statistics here.")
        return
    
    # Summary metrics
    total_domains = len(stats)
    total_pages = sum(s['total_pages'] for s in stats)
    total_successful = sum(s['successful_pages'] for s in stats)
    
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Domains", total_domains)
    col2.metric("Total Pages", total_pages)
    col3.metric("Success Rate", f"{(total_successful/total_pages*100):.1f}%" if total_pages > 0 else "0%")
    
    # Domain details
    st.subheader("Domain Statistics")
    for domain_stats in stats:
        with st.expander(f"Domain: {domain_stats['domain']}"):
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Pages", domain_stats['total_pages'])
            col2.metric("Successful", domain_stats['successful_pages'])
            col3.metric("Failed", domain_stats['failed_pages'])
            col4.metric("Success Rate", f"{domain_stats['success_rate']:.1f}%")
            
            if domain_stats['crawl_history']:
                df = pd.DataFrame(domain_stats['crawl_history'])
                df['start_time'] = pd.to_datetime(df['start_time'])
                df['end_time'] = pd.to_datetime(df['end_time'])
                
                # Memory usage chart
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=df['end_time'],
                    y=df['current_memory_usage'],
                    mode='lines+markers',
                    name='Memory Usage (MB)'
                ))
                fig.update_layout(
                    title='Memory Usage Over Time',
                    xaxis_title='Time',
                    yaxis_title='Memory Usage (MB)'
                )
                st.plotly_chart(fig, use_container_width=True)

async def update_stats(url: str, output_dir: str):
    """Update stats periodically"""
    while st.session_state.crawling:
        domain = urlparse(url).netloc
        db_path = Path(output_dir) / domain / 'stats.db'
        
        if db_path.exists():
            conn = sqlite3.connect(str(db_path))
            stats = pd.read_sql_query('SELECT * FROM crawl_stats ORDER BY start_time DESC LIMIT 1', conn)
            
            if not stats.empty:
                st.session_state.stats = {
                    'total_urls': stats['total_urls'].iloc[0],
                    'successful': stats['successful'].iloc[0],
                    'failed': stats['failed'].iloc[0],
                    'current_memory_usage': stats['current_memory_usage'].iloc[0]
                }
                
                st.session_state.memory_usage.append({
                    'time': datetime.now().strftime('%H:%M:%S'),
                    'usage': stats['current_memory_usage'].iloc[0]
                })
                
                if len(st.session_state.memory_usage) > 50:
                    st.session_state.memory_usage.pop(0)
                
            conn.close()
            st.session_state.last_update = datetime.now()
        
        await asyncio.sleep(2)

async def start_crawling(url: str, output_dir: str, test_mode: bool = False):
    """Start the crawling process"""
    try:
        scraper = WebScraper(
            base_url=url,
            output_dir=output_dir,
            max_concurrent=5,
            requests_per_second=2.0,
            test_mode=test_mode
        )
        
        # If patterns are selected, filter URLs
        if st.session_state.selected_patterns and st.session_state.analyzer:
            scraper.override_discovered_urls = st.session_state.analyzer.filter_urls(st.session_state.selected_patterns)
        
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
    
    # Navigation
    page = st.sidebar.radio("Navigation", ["Crawler", "History"])
    
    if page == "Crawler":
        # Input form
        with st.form("crawl_form"):
            url = st.text_input("Enter website URL to crawl")
            output_dir = st.text_input("Output Directory", "/tmp/webscraper")
            
            # Test mode checkbox with tooltip
            test_mode = st.checkbox(
                "Test Mode (limit to 15 pages)", 
                value=True,
                help="Enable test mode to limit crawling to maximum 15 pages. Useful for testing before full crawl."
            )
            
            analyze_button = st.form_submit_button("Analyze Sitemap")
            if analyze_button and url:
                with st.spinner("Analyzing sitemap..."):
                    asyncio.run(analyze_sitemap(url))
        
        # Pattern selection
        if st.session_state.url_patterns:
            display_pattern_selection()
            
            if st.button("Start Crawling", disabled=not st.session_state.selected_patterns):
                try:
                    # Validate URL
                    result = urlparse(url)
                    if not all([result.scheme, result.netloc]):
                        st.error("Invalid URL format")
                        return
                    
                    # Create output directory
                    Path(output_dir).mkdir(parents=True, exist_ok=True)
                    
                    # Start crawling
                    with st.spinner("Crawling in progress..."):
                        asyncio.run(start_crawling(url, output_dir, test_mode))
                    
                except Exception as e:
                    st.error(f"Error: {str(e)}")
        
        # Display current status if crawling is in progress or completed
        if st.session_state.stats:
            st.header("Current Crawl Status")
            
            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total URLs", st.session_state.stats.get('total_urls', 0))
            col2.metric("Successful", st.session_state.stats.get('successful', 0))
            col3.metric("Failed", st.session_state.stats.get('failed', 0))
            col4.metric("Memory Usage (MB)", f"{st.session_state.stats.get('current_memory_usage', 0):.1f}")
            
            # Memory usage chart
            if st.session_state.memory_usage:
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=[entry['time'] for entry in st.session_state.memory_usage],
                    y=[entry['usage'] for entry in st.session_state.memory_usage],
                    mode='lines+markers',
                    name='Memory Usage (MB)'
                ))
                fig.update_layout(
                    title='Memory Usage Over Time',
                    xaxis_title='Time',
                    yaxis_title='Memory Usage (MB)'
                )
                st.plotly_chart(fig, use_container_width=True)
            
            if st.session_state.last_update:
                st.caption(f"Last updated: {st.session_state.last_update.strftime('%H:%M:%S')}")
    
    else:  # History page
        display_crawl_history()

if __name__ == "__main__":
    main()
