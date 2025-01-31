"""Main entry point for the Streamlit application."""

import streamlit as st

def initialize_session_state():
    """Initialize all session state variables"""
    if 'discovered_urls' not in st.session_state:
        st.session_state.discovered_urls = None
    if 'domain' not in st.session_state:
        st.session_state.domain = None
    if 'crawl_results' not in st.session_state:
        st.session_state.crawl_results = None
    if 'crawled_domains' not in st.session_state:
        st.session_state.crawled_domains = set()
    if 'messages' not in st.session_state:
        st.session_state.messages = []

def main():
    """Main application entry point"""
    st.set_page_config(
        page_title="Web Crawler & RAG Chat",
        page_icon="🌐",
        layout="wide"
    )
    
    # Initialize session state
    initialize_session_state()
    
    # Render main page content
    st.title("🌐 Web Crawler & RAG Chat")
    st.markdown("""
    Welcome! Choose from the pages in the sidebar:
    
    * **🕷️ Crawler**: Discover and crawl web pages
    * **💬 Chat**: Chat with an AI assistant about the crawled content
    
    Start by using the Crawler to gather content, then use Chat to interact with it!
    """)

if __name__ == "__main__":
    main()