import streamlit as st
from utils.snowflake import SnowflakeManager
from utils.api import FastAPIClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

def init_session_state():
    """Initialize session state variables"""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "api_client" not in st.session_state:
        st.session_state.api_client = FastAPIClient(st.secrets["api_url"])
    if "snowflake" not in st.session_state:
        st.session_state.snowflake = SnowflakeManager()

def main():
    """Main Streamlit application"""
    # Set page config
    st.set_page_config(
        page_title="Web Crawler & RAG Assistant",
        page_icon="🕷️",
        layout="wide"
    )
    
    # Initialize session state
    init_session_state()
    
    # Display header
    st.title("🕷️ Web Crawler & RAG Assistant")
    st.markdown("""
    Welcome! This application provides two main features:
    1. Web Crawling: Discover and crawl web content
    2. Chat Assistant: Query your documentation using RAG
    """)
    
    # Main tabs
    tab1, tab2 = st.tabs(["🌐 Web Crawler", "💬 Chat Assistant"])
    
    with tab1:
        from pages import crawler
        crawler.show()
    
    with tab2:
        from pages import chat
        chat.show()

if __name__ == "__main__":
    main()
