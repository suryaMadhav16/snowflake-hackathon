import streamlit as st
import logging
import traceback
from datetime import datetime
from services.api_client import APIClient
from components.url_input import render_url_input
from components.url_selector import render_url_selector
from components.results import render_results

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_frontend.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def initialize_session_state():
    if "discovered_urls" not in st.session_state:
        st.session_state.discovered_urls = None
    if "domain" not in st.session_state:
        st.session_state.domain = None
    if "crawl_results" not in st.session_state:
        st.session_state.crawl_results = None
    if "crawled_domains" not in st.session_state:
        st.session_state.crawled_domains = set()
    if "selected_urls" not in st.session_state:
        st.session_state.selected_urls = None
    if "exclude_patterns" not in st.session_state:
        st.session_state.exclude_patterns = None

@st.cache_resource
def get_api_client():
    return APIClient()

def main():
    logger.info("Starting crawler application")
    st.title("🕷️ Web Crawler")
    # Initialize components
    initialize_session_state()
    api_client = get_api_client()    
    
    # URL Input and Discovery
    url, mode, discover_clicked = render_url_input()
    
    if discover_clicked:
        with st.spinner("Discovering URLs..."):
            try:
                response = api_client.discover_urls(url, mode)
                # Clear any existing selections
                st.session_state.selection_df = None
                st.session_state.selected_urls = None
                st.session_state.exclude_patterns = None
                # Update with new discoveries
                st.session_state.discovered_urls = response["urls"]
                st.session_state.domain = response["domain"]
                st.success(f"Found {len(response['urls'])} URLs")
            except Exception as e:
                error_msg = f"Failed to discover URLs: {str(e)}"
                logger.error(f"{error_msg}\n{traceback.format_exc()}")
                st.error(error_msg)
                st.session_state.discovered_urls = None
                st.session_state.domain = None
    
    # URL Selection and Crawling
    if st.session_state.discovered_urls:
        selected_urls, exclude_patterns = render_url_selector(
            st.session_state.discovered_urls,
            st.session_state.domain
        )
        
        # Store selected URLs and patterns in session state
        selected_urls = st.session_state.selected_urls
        exclude_patterns = st.session_state.exclude_patterns
        
        if selected_urls:
            if st.button("Start Crawling", type="primary"):
                with st.spinner("Crawling selected URLs..."):
                    try:
                        # Always use session state values
                        response = api_client.crawl_urls(
                            st.session_state.selected_urls,
                            st.session_state.exclude_patterns
                        )
                        st.session_state.crawl_results = response["results"]                                            
                                
                    except Exception as e:
                        error_msg = f"Crawling failed: {str(e)}"
                        logger.error(f"{error_msg}\n{traceback.format_exc()}")
                        st.error(error_msg)
                        st.session_state.crawl_results = None
        
        # Display Results
        if st.session_state.crawl_results:
            render_results(st.session_state.crawl_results)
            
            if st.button("Start Over"):
                # Clear all session state
                st.session_state.discovered_urls = None
                st.session_state.domain = None
                st.session_state.crawl_results = None
                st.session_state.selected_urls = None
                st.session_state.exclude_patterns = None
                st.rerun()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Critical application error: {str(e)}", exc_info=True)
        st.error("A critical error occurred. Please check the logs for details.")
