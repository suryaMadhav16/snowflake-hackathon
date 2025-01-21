import asyncio
import json
import logging
from typing import Callable, Dict, Optional
import websockets
from websockets.exceptions import ConnectionClosed

logger = logging.getLogger(__name__)

class WebSocketClient:
    """WebSocket client for real-time updates"""
    
    def __init__(self, base_url: str):
        """Initialize WebSocket client with base URL"""
        self.base_url = base_url.rstrip('/')
        if self.base_url.startswith('http'):
            self.base_url = f"ws{'s' if 'https' in self.base_url else ''}://{self.base_url.split('://', 1)[1]}"
        self.api_version = "v1"
        self.connections = {}
        self.active = True
    
    @property
    def api_url(self) -> str:
        """Get base API URL"""
        return f"{self.base_url}/api/{self.api_version}"
    
    async def connect_metrics(
        self,
        task_id: str,
        callback: Callable[[Dict], None]
    ):
        """Connect to metrics WebSocket"""
        uri = f"{self.api_url}/ws/metrics/{task_id}"
        await self._connect("metrics", uri, callback)
    
    async def connect_progress(
        self,
        task_id: str,
        callback: Callable[[Dict], None]
    ):
        """Connect to progress WebSocket"""
        uri = f"{self.api_url}/ws/progress/{task_id}"
        await self._connect("progress", uri, callback)
    
    async def _connect(
        self,
        connection_type: str,
        uri: str,
        callback: Callable[[Dict], None]
    ):
        """Create and maintain WebSocket connection"""
        while self.active:
            try:
                async with websockets.connect(uri) as websocket:
                    self.connections[connection_type] = websocket
                    try:
                        async for message in websocket:
                            if not self.active:
                                break
                            try:
                                data = json.loads(message)
                                callback(data)
                            except json.JSONDecodeError as e:
                                logger.error(f"WebSocket message parse error: {str(e)}")
                    except ConnectionClosed:
                        logger.info(f"{connection_type} WebSocket connection closed")
                    except Exception as e:
                        logger.error(f"WebSocket error: {str(e)}")
                    finally:
                        self.connections.pop(connection_type, None)
                        
                if not self.active:
                    break
                    
                # Connection lost, wait before retrying
                await asyncio.sleep(1)
                
            except Exception as e:
                logger.error(f"WebSocket connection error: {str(e)}")
                if not self.active:
                    break
                await asyncio.sleep(5)  # Wait longer before retrying
    
    async def disconnect(self, connection_type: Optional[str] = None):
        """Disconnect WebSocket connection(s)"""
        self.active = False
        if connection_type:
            websocket = self.connections.get(connection_type)
            if websocket:
                await websocket.close()
                self.connections.pop(connection_type, None)
        else:
            for websocket in self.connections.values():
                await websocket.close()
            self.connections.clear()
