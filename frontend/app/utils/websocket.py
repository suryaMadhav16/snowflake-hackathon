import asyncio
import json
import logging
import time
from typing import Callable, Dict, Optional
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatusCode, InvalidMessage
import streamlit as st

logger = logging.getLogger(__name__)

class WebSocketClient:
    """WebSocket client for real-time updates"""
    
    def __init__(self, base_url: str):
        """Initialize WebSocket client with base URL"""
        self.base_url = base_url.rstrip('/')
        # Convert HTTP URL to WebSocket URL
        if self.base_url.startswith('http'):
            self.base_url = f"ws{'s' if 'https' in self.base_url else ''}://{self.base_url.split('://', 1)[1]}"
        logger.debug(f"WebSocket base URL: {self.base_url}")
        self.api_version = "v1"
        self.connections = {}
        self.connection_status = {}
        self.active = True
    
    def _get_ws_url(self, endpoint: str) -> str:
        """Construct WebSocket URL"""
        return f"{self.base_url}/api/{self.api_version}/{endpoint}"
    
    def _update_status(self, connection_type: str, status: Dict):
        """Update connection status"""
        current = self.get_connection_status(connection_type)
        current.update(status)
        self.connection_status[connection_type] = current
        logger.debug(f"Updated {connection_type} status: {current}")
    
    def get_connection_status(self, connection_type: str) -> Dict:
        """Get connection status"""
        return self.connection_status.get(connection_type, {
            "connected": False,
            "last_error": None,
            "last_message_time": None,
            "connection_url": None,
            "connection_id": None,
            "retry_count": 0,
            "connected_at": None,
            "client_info": None,
            "total_messages": 0,
            "connection_attempts": 0
        })
    
    async def connect_metrics(self, task_id: str, callback: Callable[[Dict], None]):
        """Connect to metrics WebSocket"""
        uri = self._get_ws_url(f"ws/metrics/{task_id}")
        logger.debug(f"Connecting to metrics WebSocket at {uri}")
        await self._connect("metrics", uri, callback)
    
    async def connect_progress(self, task_id: str, callback: Callable[[Dict], None]):
        """Connect to progress WebSocket"""
        uri = self._get_ws_url(f"ws/progress/{task_id}")
        logger.debug(f"Connecting to progress WebSocket at {uri}")
        await self._connect("progress", uri, callback)
    
    async def test_connection(self) -> bool:
        """Test WebSocket connectivity using debug endpoint"""
        uri = self._get_ws_url("ws/debug")
        logger.debug(f"Testing WebSocket connection at {uri}")
        
        try:
            async with websockets.connect(uri) as websocket:
                # Set initial connection info
                self._update_status("debug", {
                    "connected": True,
                    "connecting": False,
                    "connected_at": time.time(),
                    "connection_url": uri
                })
                
                # Wait for initial connection message
                message = await websocket.recv()
                data = json.loads(message)
                
                if data.get("status") == "connected":
                    logger.info("WebSocket test connection successful")
                    return True
                else:
                    logger.error(f"Unexpected connection response: {data}")
                    return False
                    
        except Exception as e:
            logger.error(f"WebSocket test connection failed: {str(e)}")
            self._update_status("debug", {
                "connected": False,
                "last_error": str(e)
            })
            return False
    
    async def _connect(self, connection_type: str, uri: str, callback: Callable[[Dict], None]):
        """Create and maintain WebSocket connection"""
        retry_count = 0
        max_retries = 5
        initial_retry_delay = 1.0
        
        while self.active and (retry_count < max_retries):
            try:
                logger.debug(f"Attempting to connect to {connection_type} WebSocket ({retry_count + 1}/{max_retries})")
                self._update_status(connection_type, {
                    "connection_attempts": retry_count + 1,
                    "connecting": True,
                    "last_attempt": time.time(),
                    "connection_url": uri
                })
                
                async with websockets.connect(uri) as websocket:
                    # Store the connection
                    self.connections[connection_type] = websocket
                    connect_time = time.time()
                    
                    # Update status with connection info
                    self._update_status(connection_type, {
                        "connected": True,
                        "connecting": False,
                        "connected_at": connect_time,
                        "last_error": None,
                        "connection_id": id(websocket)
                    })
                    
                    logger.info(f"Successfully connected to {connection_type} WebSocket")
                    
                    # Message processing loop
                    try:
                        async for message in websocket:
                            if not self.active:
                                break
                                
                            try:
                                data = json.loads(message)
                                self._update_status(connection_type, {
                                    "last_message_time": time.time(),
                                    "total_messages": self.get_connection_status(connection_type)["total_messages"] + 1
                                })
                                await callback(data)
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse WebSocket message: {str(e)}")
                                self._update_status(connection_type, {
                                    "last_error": f"Message parse error: {str(e)}"
                                })
                    
                    except ConnectionClosed as e:
                        logger.warning(f"{connection_type} WebSocket connection closed: {str(e)}")
                        self._update_status(connection_type, {
                            "connected": False,
                            "last_error": f"Connection closed: {str(e)}"
                        })
                        raise  # Propagate to trigger retry
            
            except Exception as e:
                logger.error(f"WebSocket error ({connection_type}): {str(e)}")
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": str(e)
                })
                
                retry_count += 1
                if retry_count < max_retries:
                    # Exponential backoff with jitter
                    delay = min(initial_retry_delay * (2 ** (retry_count - 1)), 30)
                    jitter = delay * 0.1
                    delay = delay + (asyncio.get_event_loop().time() % jitter)
                    logger.debug(f"Retrying {connection_type} WebSocket in {delay:.2f}s ({retry_count}/{max_retries})")
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"Max retries ({max_retries}) reached for {connection_type} WebSocket")
                    self._update_status(connection_type, {
                        "retry_count": retry_count,
                        "last_error": f"Maximum retries ({max_retries}) reached"
                    })
            finally:
                # Clean up connection reference
                self.connections.pop(connection_type, None)
    
    async def disconnect(self, connection_type: Optional[str] = None):
        """Disconnect WebSocket connection(s)"""
        self.active = False
        
        if connection_type:
            # Disconnect specific connection
            websocket = self.connections.get(connection_type)
            if websocket:
                await websocket.close()
                self.connections.pop(connection_type, None)
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": "Disconnected by client"
                })
        else:
            # Disconnect all connections
            for conn_type, websocket in self.connections.items():
                await websocket.close()
                self._update_status(conn_type, {
                    "connected": False,
                    "last_error": "Disconnected by client"
                })
            self.connections.clear()