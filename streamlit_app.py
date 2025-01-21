import os
import sys
import logging
import streamlit as st
from pathlib import Path
import subprocess

# Configure logging first
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def check_playwright_installation():
    try:
        # Check if playwright browsers are installed
        result = subprocess.run(['playwright', 'show-browsers'], 
                              capture_output=True, 
                              text=True)
        if "chromium" in result.stdout:
            logger.info("Playwright browsers are already installed")
            return True
    except FileNotFoundError:
        logger.warning("Playwright command not found")
    except Exception as e:
        logger.error(f"Error checking Playwright installation: {e}")
    return False

def install_playwright():
    try:
        logger.info("Installing Playwright browsers...")
        subprocess.run(['playwright', 'install'], check=True)
        logger.info("Playwright browsers installed successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to install Playwright browsers: {e}")
        return False

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

# Check and install Playwright if needed
if not check_playwright_installation():
    if not install_playwright():
        st.error("Failed to install Playwright. Please check the logs for details.")
        st.stop()

# Import our application
from src.ui import main

if __name__ == "__main__":
    main()
