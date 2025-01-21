import streamlit as st
import pandas as pd
from typing import List, Dict

def render_results(results: List[Dict]):
    """Render crawling results"""
    
    st.write("### Crawling Results")
    
    # Process results into DataFrame
    data = []
    for result in results:
        row = {
            'URL': result['url'],
            'Status': '✅ Success' if result['success'] else '❌ Failed',
            'Files': len(result['files']) if result['success'] else 0,
            'RAG Status': '🔄 Processing'  # Will be updated when RAG processing is done
        }
        if not result['success']:
            row['Error'] = result.get('error_message', 'Unknown error')
        data.append(row)
    
    df = pd.DataFrame(data)
    
    # Show summary metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total = len(results)
        successful = sum(1 for r in results if r['success'])
        st.metric("Total URLs", total)
        
    with col2:
        success_rate = (successful / total * 100) if total > 0 else 0
        st.metric("Success Rate", f"{success_rate:.1f}%")
        
    with col3:
        total_files = sum(len(r['files']) for r in results if r['success'])
        st.metric("Saved Files", total_files)
        
    with col4:
        st.metric("RAG Status", "Processing")
    
    # Show results table
    st.write("#### Details")
    st.dataframe(
        df,
        column_config={
            "URL": st.column_config.TextColumn(
                "URL",
                help="Crawled URL",
                width="large"
            ),
            "Status": st.column_config.TextColumn(
                "Status",
                help="Crawling status"
            ),
            "Files": st.column_config.NumberColumn(
                "Saved Files",
                help="Number of files saved"
            ),
            "RAG Status": st.column_config.TextColumn(
                "RAG Status",
                help="RAG processing status"
            )
        },
        hide_index=True
    )
    
    # Show saved files
    st.write("#### Saved Files")
    for result in results:
        if result['success'] and result['files']:
            with st.expander(f"Files for {result['url']}", expanded=False):
                for file_type, file_path in result['files'].items():
                    st.text(f"{file_type}: {file_path}")
    
    # Show errors if any
    errors = [r for r in results if not r['success']]
    if errors:
        st.write("#### Errors")
        for error in errors:
            st.error(f"{error['url']}: {error.get('error_message', 'Unknown error')}")
            
    st.info("""
    🔍 What's happening:
    1. Content has been crawled successfully
    2. Processing content for RAG (chunking & vectorization)
    3. Once complete, you can use the Chat page to interact with the content
    """)
