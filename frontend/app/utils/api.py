import httpx
from typing import Dict, List, Optional
import logging

logger = logging.getLogger(__name__)

class FastAPIClient:
    """FastAPI client for interacting with the backend"""
    
    def __init__(self, base_url: str):
        """Initialize API client with base URL"""
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient()
        self.api_version = "v1"
    
    @property
    def api_url(self) -> str:
        """Get base API URL"""
        return f"{self.base_url}/api/{self.api_version}"
    
    async def discover_urls(
        self,
        url: str,
        mode: str = "full",
        settings: Optional[Dict] = None
    ) -> Dict:
        """Start URL discovery process"""
        try:
            response = await self.client.post(
                f"{self.api_url}/discover",
                params={"url": url, "mode": mode},
                json={"settings": settings} if settings else None
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"URL discovery error: {str(e)}")
            raise
    
    async def start_crawling(
        self,
        urls: List[str],
        settings: Optional[Dict] = None
    ) -> Dict:
        """Start crawling process"""
        try:
            response = await self.client.post(
                f"{self.api_url}/crawl",
                json={
                    "urls": urls,
                    "settings": settings
                }
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Crawling error: {str(e)}")
            raise
    
    async def get_status(self, task_id: str) -> Dict:
        """Get task status"""
        try:
            response = await self.client.get(
                f"{self.api_url}/status/{task_id}"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Status check error: {str(e)}")
            raise
    
    async def get_results(self, url: str) -> Dict:
        """Get crawling results for URL"""
        try:
            response = await self.client.get(
                f"{self.api_url}/results/{url}"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Results retrieval error: {str(e)}")
            raise
    
    async def get_discovery_results(self, task_id: str) -> Dict:
        """Get URL discovery results"""
        try:
            response = await self.client.get(
                f"{self.api_url}/discovery/{task_id}"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Discovery results error: {str(e)}")
            raise
    
    async def get_files(
        self,
        url: str,
        file_type: Optional[str] = None
    ) -> List[Dict]:
        """Get list of saved files"""
        try:
            params = {"file_type": file_type} if file_type else None
            response = await self.client.get(
                f"{self.api_url}/files/{url}",
                params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"File listing error: {str(e)}")
            raise
    
    async def get_file_content(
        self,
        url: str,
        file_type: str
    ) -> Dict:
        """Get file content"""
        try:
            response = await self.client.get(
                f"{self.api_url}/file-content/{url}",
                params={"file_type": file_type}
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"File content error: {str(e)}")
            raise
    
    async def get_stats(self) -> Dict:
        """Get crawler statistics"""
        try:
            response = await self.client.get(
                f"{self.api_url}/stats"
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Stats error: {str(e)}")
            raise
    
    async def close(self):
        """Close the HTTP client"""
        await self.client.aclose()
