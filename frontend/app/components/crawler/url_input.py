import streamlit as st
import validators
from typing import Optional, Tuple

class URLInput:
    """Component for URL input and validation"""
    
    @staticmethod
    def render() -> Tuple[Optional[str], bool]:
        """
        Render URL input component
        Returns: (url, is_valid)
        """
        st.subheader("Enter URL")
        
        url = st.text_input(
            "Target URL",
            placeholder="https://example.com",
            help="Enter the website URL you want to crawl"
        )
        
        if not url:
            return None, False
        
        # Validate URL
        if not url.startswith(('http://', 'https://')):
            url = f"https://{url}"
        
        is_valid = validators.url(url)
        
        if url and not is_valid:
            st.error("Please enter a valid URL")
            return url, False
        
        if url:
            st.info("URL will be crawled with current settings")
        
        return url, is_valid
