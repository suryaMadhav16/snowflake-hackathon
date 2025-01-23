"""Main entry point for the Web Crawler Streamlit application.

This module initializes the Streamlit web interface and handles the setup of required
dependencies, particularly Playwright for web automation. It performs the following:
1. Configures logging
2. Checks and installs Playwright browsers if needed
3. Sets up the Streamlit page configuration
4. Manages Python path for imports
5. Launches the main application UI

The application requires Playwright browsers to be installed for web crawling operations.
If not present, it will attempt to install them automatically.

Example:
    To run the application:
        $ streamlit run streamlit_app.py
"""

import os
import sys
import logging
import streamlit as st
from pathlib import Path
import subprocess
from src.ui import main
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

def check_playwright_installation() -> bool:
    """Check if Playwright browsers are installed.

    Uses the Playwright CLI to verify if browsers, particularly Chromium,
    are installed and available for use.

    Returns:
        bool: True if Playwright browsers are installed, False otherwise.

    Example:
        >>> if check_playwright_installation():
        ...     print("Playwright is ready to use")
        ... else:
        ...     print("Playwright needs to be installed")
    """
    try:
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

def install_playwright() -> bool:
    """Install Playwright browsers.

    Attempts to install Playwright browsers using the Playwright CLI.
    This is required for web automation and crawling functionality.

    Returns:
        bool: True if installation was successful, False otherwise.

    Example:
        >>> if not check_playwright_installation():
        ...     success = install_playwright()
        ...     if success:
        ...         print("Installation successful")
        ...     else:
        ...         print("Installation failed")
    """
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



if __name__ == "__main__":
    main()
