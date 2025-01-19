import os
import sys
import logging
import streamlit as st
from pathlib import Path

# Set page config first before any other Streamlit commands
st.set_page_config(
    page_title="Web Crawler",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Add src directory to Python path
src_path = str(Path(__file__).parent / 'src')
if src_path not in sys.path:
    sys.path.append(src_path)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)

# Import our application
from src.ui import main

if __name__ == "__main__":
    main()