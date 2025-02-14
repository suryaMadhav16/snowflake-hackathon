import streamlit as st
import pandas as pd
from typing import List, Tuple, Optional

def render_url_selector(urls: List[str], domain: str) -> Tuple[List[str], Optional[List[str]]]:
    """Render URL selection interface with pattern filtering"""
    
    st.write("### Select URLs to Crawl")

    # (A) Initialize the DataFrame in session state once
    if "selection_df" not in st.session_state or st.session_state.selection_df is None:
        st.session_state.selection_df = pd.DataFrame({
            'URL': urls,
            'Selected': [True] * len(urls)  # brand-new state, only once
        })

    # Make a local copy to apply patterns to
    df = st.session_state.selection_df.copy()
    
    # (B) Persist pattern text in session state so it doesn't vanish on rerun
    if "pattern_text" not in st.session_state:
        st.session_state.pattern_text = ""

    # Pattern exclusion UI
    st.write("#### Filter URLs")
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.session_state.pattern_text = st.text_area(
            "Exclude Patterns (one per line)",
            value=st.session_state.pattern_text,  # persist user‐entered text
            help="URLs matching these patterns will be excluded",
            height=100
        )
    
    with col2:
        st.write("")
        st.write("")
        if st.button("Apply Patterns"):
            # Convert text to list of patterns
            pattern_text = st.session_state.pattern_text
            if pattern_text:
                patterns = [p.strip() for p in pattern_text.split('\n') if p.strip()]
                # Update 'Selected' for matching URLs
                for pattern in patterns:
                    df.loc[df['URL'].str.contains(pattern, case=False), 'Selected'] = False

    # (C) Show the Data Editor with a stable key
    st.write("#### URL Selection")
    edited_df = st.data_editor(
        df,
        key="url_selector_data",  # stable key 
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
    
    # (D) Save the user-edited DataFrame back into session state
    st.session_state.selection_df = edited_df

    # (E) Convert final state -> selected list
    selected_urls = edited_df[edited_df['Selected']]['URL'].tolist()

    # (F) Build your exclude_patterns from session's pattern_text
    exclude_patterns = [
        p.strip() for p in st.session_state.pattern_text.split('\n') 
        if p.strip()
    ] or None

    st.session_state.selected_urls = selected_urls    
    st.session_state.exclude_patterns = exclude_patterns

    # Show summary
    st.write(f"Selected {len(selected_urls)} out of {len(urls)} URLs")    
    
    return selected_urls, exclude_patterns
