import streamlit as st
from typing import Tuple

def render_url_input() -> Tuple[str, str, bool]:
    """Render URL input and mode selection"""
    
    st.write("### Enter URL to Crawl")
    
    url = st.text_input(
        "Website URL",
        help="Enter the URL of the website you want to crawl"
    )
    
    col1, col2 = st.columns([2, 1])
    with col1:
        mode = st.radio(
            "Select Mode",
            ["Single Page", "Full Website"],
            help=("Single Page: Only crawl the given URL\n"
                  "Full Website: Discover and crawl multiple pages")
        )
    
    with col2:
        discover_button = st.button(
            "Discover URLs",
            help="Start URL discovery process",
            type="primary",
            disabled=not url
        )
    
    return url, "single" if mode == "Single Page" else "full", discover_button