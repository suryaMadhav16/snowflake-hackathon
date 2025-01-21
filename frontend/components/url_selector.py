import streamlit as st
import pandas as pd
from typing import List, Tuple, Optional

def render_url_selector(urls: List[str], domain: str) -> Tuple[List[str], Optional[List[str]]]:
    """Render URL selection interface with pattern filtering"""
    
    st.write("### Select URLs to Crawl")
    
    # Create DataFrame with URLs
    df = pd.DataFrame({
        'URL': urls,
        'Selected': [True] * len(urls)  # All selected by default
    })
    
    # Pattern exclusion
    st.write("#### Filter URLs")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        pattern_text = st.text_area(
            "Exclude Patterns (one per line)",
            help="URLs matching these patterns will be excluded",
            height=100
        )
    
    with col2:
        st.write("")
        st.write("")
        if st.button("Apply Patterns"):
            # Convert text to list of patterns
            if pattern_text:
                patterns = [p.strip() for p in pattern_text.split('\n') if p.strip()]
                # Update selection based on patterns
                for pattern in patterns:
                    df.loc[df['URL'].str.contains(pattern, case=False), 'Selected'] = False
    
    # URL selection table
    st.write("#### URL Selection")
    edited_df = st.data_editor(
        df,
        column_config={
            "URL": st.column_config.TextColumn(
                "URL",
                help="URLs discovered from the website",
                width="large"
            ),
            "Selected": st.column_config.CheckboxColumn(
                "Select",
                help="Select URLs to crawl"
            )
        },
        disabled=["URL"],
        hide_index=True
    )
    
    # Get selected URLs and patterns
    selected_urls = edited_df[edited_df['Selected']]['URL'].tolist()
    exclude_patterns = [p.strip() for p in pattern_text.split('\n') if p.strip()] if pattern_text else None
    
    # Show summary
    st.write(f"Selected {len(selected_urls)} out of {len(urls)} URLs")
    
    return selected_urls, exclude_patterns