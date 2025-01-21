import requests
from typing import List, Dict, Optional
import streamlit as st

class APIClient:
    """Client for interacting with the crawler backend API"""
    
    def __init__(self):
        """Initialize API client with base URL from secrets"""
        self.base_url = st.secrets["api_url"]
        if not self.base_url.endswith('/'):
            self.base_url += '/'
    
    def _make_request(
        self,
        method: str,
        endpoint: str,
        data: dict = None,
        timeout: int = 300  # Increased timeout to 5 minutes
    ) -> Dict:
        """Make HTTP request to API"""
        url = f"{self.base_url}api/v1/{endpoint}"
        
        try:
            with st.spinner("Processing request..."):
                response = requests.request(
                    method=method,
                    url=url,
                    json=data,
                    timeout=timeout
                )
                response.raise_for_status()
                return response.json()
            
        except requests.exceptions.Timeout:
            st.error(
                "Request timed out. This usually happens with large websites. "
                "Try with fewer URLs or contact administrator to increase timeout."
            )
            raise
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                st.error("No URLs found. Please check the URL and try again.")
            else:
                st.error(f"API Error: {str(e)}")
            raise
            
        except requests.exceptions.ConnectionError:
            st.error("Could not connect to API. Please check if the backend is running.")
            raise
            
        except Exception as e:
            st.error(f"Unexpected error: {str(e)}")
            raise
    
    def discover_urls(self, url: str, mode: str = "full") -> Dict[str, List[str]]:
        """Discover URLs from website"""
        data = {
            "url": url,
            "mode": mode
        }
        # Use 5 minute timeout for discovery
        return self._make_request("POST", "discover", data, timeout=300)
    
    def crawl_urls(
        self,
        urls: List[str],
        exclude_patterns: Optional[List[str]] = None
    ) -> Dict[str, List[Dict]]:
        """Crawl selected URLs"""
        data = {
            "urls": urls,
            "exclude_patterns": exclude_patterns or []
        }
        # Use 10 minute timeout for crawling
        return self._make_request("POST", "crawl", data, timeout=600)