import streamlit as st
from typing import Dict, List

class CrawlerSettings:
    """Component for crawler configuration settings"""
    
    @staticmethod
    def render() -> Dict:
        """Render crawler settings form"""
        
        with st.expander("🛠️ Crawler Settings", expanded=False):
            col1, col2 = st.columns(2)
            
            with col1:
                browser_type = st.selectbox(
                    "Browser Type",
                    options=["chromium", "firefox", "webkit"],
                    index=0,
                    help="Select the browser engine for crawling"
                )
                
                performance_mode = st.selectbox(
                    "Performance Mode",
                    options=["Low", "Medium", "High"],
                    index=1,
                    help="Adjust crawling speed and resource usage"
                )
                
                batch_size = st.number_input(
                    "Batch Size",
                    min_value=1,
                    max_value=50,
                    value=10,
                    help="Number of URLs to process in parallel"
                )
            
            with col2:
                max_depth = st.number_input(
                    "Max Depth",
                    min_value=1,
                    max_value=10,
                    value=3,
                    help="Maximum depth of URL discovery"
                )
                
                exclusion_patterns = st.text_area(
                    "Exclusion Patterns",
                    height=100,
                    help="Enter regex patterns to exclude (one per line)"
                ).strip().split('\n')
                
                exclusion_patterns = [p for p in exclusion_patterns if p]
            
            # Content settings
            st.subheader("Content Settings")
            col3, col4, col5 = st.columns(3)
            
            with col3:
                save_images = st.checkbox(
                    "Save Images",
                    value=True,
                    help="Download and save images"
                )
            
            with col4:
                capture_screenshots = st.checkbox(
                    "Capture Screenshots",
                    value=True,
                    help="Take screenshots of pages"
                )
            
            with col5:
                generate_pdfs = st.checkbox(
                    "Generate PDFs",
                    value=True,
                    help="Generate PDF versions of pages"
                )
        
        # Return settings dict
        return {
            "browser_type": browser_type,
            "performance_mode": performance_mode,
            "batch_size": batch_size,
            "max_depth": max_depth,
            "exclusion_patterns": exclusion_patterns,
            "save_images": save_images,
            "capture_screenshots": capture_screenshots,
            "generate_pdfs": generate_pdfs
        }
