import streamlit as st
from services.api_client import APIClient
from components.url_input import render_url_input
from components.url_selector import render_url_selector
from components.results import render_results

# Initialize API client
@st.cache_resource
def get_api_client():
    return APIClient()

def main():
    st.title("Web Crawler")
    
    # Initialize session state
    if 'discovered_urls' not in st.session_state:
        st.session_state.discovered_urls = None
        st.session_state.domain = None
        st.session_state.crawl_results = None
    
    api_client = get_api_client()
    
    # URL Input Phase
    url, mode, discover_clicked = render_url_input()
    
    if discover_clicked:
        with st.spinner("Discovering URLs..."):
            try:
                response = api_client.discover_urls(url, mode)
                st.session_state.discovered_urls = response['urls']
                st.session_state.domain = response['domain']
                st.success(f"Found {len(response['urls'])} URLs")
            except Exception:
                # Error already shown by API client
                st.session_state.discovered_urls = None
                st.session_state.domain = None
    
    # URL Selection Phase
    if st.session_state.discovered_urls:
        selected_urls, exclude_patterns = render_url_selector(
            st.session_state.discovered_urls,
            st.session_state.domain
        )
        
        # Crawling Phase
        if selected_urls:
            if st.button("Start Crawling", type="primary"):
                with st.spinner("Crawling selected URLs..."):
                    try:
                        response = api_client.crawl_urls(selected_urls, exclude_patterns)
                        st.session_state.crawl_results = response['results']
                    except Exception:
                        # Error already shown by API client
                        st.session_state.crawl_results = None
        
        # Results Phase
        if st.session_state.crawl_results:
            render_results(st.session_state.crawl_results)
            
            if st.button("Start Over"):
                st.session_state.discovered_urls = None
                st.session_state.domain = None
                st.session_state.crawl_results = None
                st.rerun()

if __name__ == "__main__":
    main()