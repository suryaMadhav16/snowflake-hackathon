import httpx
from typing import Dict, List, Optional, Callable
import logging
import asyncio
import time

logger = logging.getLogger(__name__)

class TaskPoller:
    """Handles polling for task updates"""
    
    def __init__(self, api_client, task_id: str, callback: Callable[[Dict], None]):
        self.api_client = api_client
        self.task_id = task_id
        self.callback = callback
        self.polling = False
        self._last_status = None
        self._error_count = 0
        self._max_errors = 3
    
    def is_complete(self, status: Dict) -> bool:
        """Check if task is complete based on status"""
        return (
            status.get("status") in ["completed", "failed"] or
            status.get("progress", 0) >= 1.0
        )
    
    async def start(self, interval: float = 1.0):
        """Start polling"""
        self.polling = True
        self._error_count = 0
        
        while self.polling:
            try:
                status = await self.api_client.get_status(self.task_id)
                
                # Only call callback if status changed
                if status != self._last_status:
                    self._last_status = status
                    await self.callback(status)
                
                # Check for completion
                if self.is_complete(status):
                    self.polling = False
                    break
                
                # Reset error count on successful poll
                self._error_count = 0
                
            except Exception as e:
                logger.error(f"Polling error: {str(e)}")
                self._error_count += 1
                
                if self._error_count >= self._max_errors:
                    logger.error("Max polling errors reached")
                    self.polling = False
                    break
            
            await asyncio.sleep(interval)
    
    def stop(self):
        """Stop polling"""
        self.polling = False

class FastAPIClient:
    """FastAPI client for interacting with the backend"""
    
    def __init__(self, base_url: str):
        """Initialize API client with base URL"""
        self.base_url = base_url.rstrip('/')
        self.client = httpx.AsyncClient()
        self.api_version = "v1"
        self.active_pollers: Dict[str, TaskPoller] = {}
    
    @property
    def api_url(self) -> str:
        """Get base API URL"""
        return f"{self.base_url}/api/{self.api_version}"

    async def start_polling(self, task_id: str, callback: Callable[[Dict], None], interval: float = 1.0):
        """Start polling for task updates"""
        if task_id in self.active_pollers:
            self.active_pollers[task_id].stop()
        
        poller = TaskPoller(self, task_id, callback)
        self.active_pollers[task_id] = poller
        asyncio.create_task(poller.start(interval))
    
    async def stop_polling(self, task_id: Optional[str] = None):
        """Stop polling for specific task or all tasks"""
        if task_id:
            if task_id in self.active_pollers:
                self.active_pollers[task_id].stop()
                del self.active_pollers[task_id]
        else:
            for poller in self.active_pollers.values():
                poller.stop()
            self.active_pollers.clear()
    
    async def discover_urls(
        self,
        url: str,
        mode: str = "full",
        settings: Optional[Dict] = None,
        on_update: Optional[Callable[[Dict], None]] = None
    ) -> Dict:
        """Start URL discovery process with optional status updates"""
        try:
            response = await self.client.post(
                f"{self.api_url}/discover",
                json={
                    "url": url,
                    "mode": mode,
                    "settings": settings
                }
            )
            response.raise_for_status()
            result = response.json()
            
            # Start polling if callback provided
            if on_update and result.get("task_id"):
                await self.start_polling(result["task_id"], on_update)
            
            return result
            
        except Exception as e:
            logger.error(f"URL discovery error: {str(e)}")
            raise
    
    async def start_crawling(
        self,
        urls: List[str],
        settings: Optional[Dict] = None,
        on_update: Optional[Callable[[Dict], None]] = None
    ) -> Dict:
        """Start crawling process with optional status updates"""
        try:
            response = await self.client.post(
                f"{self.api_url}/crawl",
                json={
                    "urls": urls,
                    "settings": settings
                }
            )
            response.raise_for_status()
            result = response.json()
            
            # Start polling if callback provided
            if on_update and result.get("task_id"):
                await self.start_polling(result["task_id"], on_update)
            
            return result
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
        """Close the HTTP client and stop all polling"""
        await self.stop_polling()
        await self.client.aclose()
