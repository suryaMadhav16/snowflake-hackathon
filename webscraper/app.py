import streamlit as st
import sqlite3
import pandas as pd
import plotly.express as px
from pathlib import Path
import asyncio
from urllib.parse import urlparse
import logging
from scraper import WebScraper

# Configure logging
logging.basicConfig(level=logging.INFO,
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

st.set_page_config(page_title="Web Scraper Dashboard", layout="wide")

def load_stats(db_path: Path) -> dict:
    """Load crawling statistics from database"""
    conn = sqlite3.connect(str(db_path))
    
    # Get overall stats
    stats = {}
    df = pd.read_sql_query('SELECT * FROM crawl_stats ORDER BY start_time DESC LIMIT 1', conn)
    if not df.empty:
        stats['total_urls'] = df['total_urls'].iloc[0]
        stats['successful'] = df['successful'].iloc[0]
        stats['failed'] = df['failed'].iloc[0]
        stats['success_rate'] = (df['successful'].iloc[0] / df['total_urls'].iloc[0] * 100) if df['total_urls'].iloc[0] > 0 else 0
    
    # Get page details
    stats['pages'] = pd.read_sql_query('''
        SELECT url, title, status, error_message, crawled_at 
        FROM pages 
        ORDER BY crawled_at DESC
    ''', conn)
    
    # Get image stats
    stats['images'] = pd.read_sql_query('''
        SELECT p.url as page_url, COUNT(i.id) as image_count
        FROM pages p
        LEFT JOIN images i ON p.id = i.page_id
        GROUP BY p.id
    ''', conn)
    
    conn.close()
    return stats

def create_progress_charts(stats: dict):
    """Create progress visualization charts"""
    col1, col2 = st.columns(2)
    
    with col1:
        # Success rate pie chart
        fig1 = px.pie(
            values=[stats['successful'], stats['failed']],
            names=['Successful', 'Failed'],
            title='Crawl Success Rate'
        )
        st.plotly_chart(fig1, use_container_width=True)
    
    with col2:
        # Image distribution bar chart
        if not stats['images'].empty:
            fig2 = px.bar(
                stats['images'],
                x='page_url',
                y='image_count',
                title='Images per Page'
            )
            fig2.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig2, use_container_width=True)

def display_crawl_status(stats: dict):
    """Display current crawling status"""
    st.header("Crawling Status")
    
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total URLs", stats.get('total_urls', 0))
    col2.metric("Successful", stats.get('successful', 0))
    col3.metric("Failed", stats.get('failed', 0))
    col4.metric("Success Rate", f"{stats.get('success_rate', 0):.1f}%")

def display_page_details(stats: dict):
    """Display detailed page information"""
    st.header("Page Details")
    
    # Success vs Failed tabs
    tab1, tab2 = st.tabs(["Successful Pages", "Failed Pages"])
    
    with tab1:
        successful_pages = stats['pages'][stats['pages']['status'] == 'success']
        if not successful_pages.empty:
            st.dataframe(successful_pages[['url', 'title', 'crawled_at']], hide_index=True)
        else:
            st.info("No successfully crawled pages yet.")
    
    with tab2:
        failed_pages = stats['pages'][stats['pages']['status'] == 'failed']
        if not failed_pages.empty:
            st.dataframe(failed_pages[['url', 'error_message', 'crawled_at']], hide_index=True)
        else:
            st.info("No failed crawls yet.")

async def start_crawling(url: str, output_dir: str):
    """Start the crawling process"""
    try:
        scraper = WebScraper(
            base_url=url,
            output_dir=output_dir,
            max_concurrent=5,
            requests_per_second=2.0
        )
        await scraper.crawl()
    except Exception as e:
        st.error(f"Error during crawling: {str(e)}")
        logger.error(f"Crawling error: {str(e)}", exc_info=True)

def main():
    st.title("Web Scraper Dashboard")
    
    # Input form
    with st.form("crawl_form"):
        url = st.text_input("Enter website URL to crawl")
        output_dir = st.text_input("Output Directory", "/tmp/webscraper")
        
        submitted = st.form_submit_button("Start Crawling")
        if submitted:
            if not url:
                st.error("Please enter a URL")
                return
                
            try:
                # Validate URL
                result = urlparse(url)
                if not all([result.scheme, result.netloc]):
                    st.error("Invalid URL format")
                    return
                
                # Create output directory
                Path(output_dir).mkdir(parents=True, exist_ok=True)
                
                # Start crawling in background
                st.info("Starting crawl... This may take a while.")
                asyncio.run(start_crawling(url, output_dir))
                
            except Exception as e:
                st.error(f"Error: {str(e)}")
                return
    
    # If crawling has started, show status
    domain = urlparse(url).netloc if url else None
    if domain:
        db_path = Path(output_dir) / domain / 'stats.db'
        if db_path.exists():
            stats = load_stats(db_path)
            
            display_crawl_status(stats)
            create_progress_charts(stats)
            display_page_details(stats)
            
            # Download buttons for reports
            st.header("Download Reports")
            if not stats['pages'].empty:
                csv = stats['pages'].to_csv(index=False)
                st.download_button(
                    label="Download Full Report (CSV)",
                    data=csv,
                    file_name="scraping_report.csv",
                    mime="text/csv"
                )

if __name__ == "__main__":
    main()
