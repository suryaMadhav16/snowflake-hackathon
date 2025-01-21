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
        if self.base_url.startswith('http'):
            self.base_url = f"ws{'s' if 'https' in self.base_url else ''}://{self.base_url.split('://', 1)[1]}"
        logger.debug(f"WebSocket base URL: {self.base_url}")
        self.api_version = "v1"
        self.connections = {}
        self.connection_status = {}
        self.active = True
    
    @property
    def api_url(self) -> str:
        """Get base API URL"""        
        return f"{self.base_url}/api/{self.api_version}"
    
    def get_connection_status(self, connection_type: str) -> Dict:
        """Get connection status"""
        status = self.connection_status.get(connection_type, {
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
        
        # Add connection URL if available
        if connection_type in self.connections:
            ws = self.connections[connection_type]
            status["connection_url"] = str(ws.url) if hasattr(ws, 'url') else None
        
        return status
    
    def _update_status(self, connection_type: str, status: Dict):
        """Update connection status"""
        current = self.get_connection_status(connection_type)
        current.update(status)
        self.connection_status[connection_type] = current
        logger.debug(f"Updated {connection_type} status: {current}")
    
    async def connect_metrics(
        self,
        task_id: str,
        callback: Callable[[Dict], None]
    ):
        """Connect to metrics WebSocket"""
        uri = f"{self.api_url}/ws/metrics/{task_id}"
        logger.debug(f"Connecting to metrics WebSocket at {uri}")
        await self._connect("metrics", uri, callback)
    
    async def connect_progress(
        self,
        task_id: str,
        callback: Callable[[Dict], None]
    ):
        """Connect to progress WebSocket"""
        uri = f"{self.api_url}/ws/progress/{task_id}"
        logger.debug(f"Connecting to progress WebSocket at {uri}")
        await self._connect("progress", uri, callback)
    
    async def _connect(
        self,
        connection_type: str,
        uri: str,
        callback: Callable[[Dict], None]
    ):
        """Create and maintain WebSocket connection"""
        retry_count = 0
        max_retries = 5
        initial_retry_delay = 1.0  # seconds
        
        while self.active:
            try:
                logger.debug(f"Attempting to connect to {connection_type} WebSocket ({retry_count + 1}/{max_retries if max_retries else 'unlimited'})")
                self._update_status(connection_type, {
                    "connection_attempts": retry_count + 1,
                    "connecting": True,
                    "last_attempt": time.time()
                })
                
                async with websockets.connect(uri) as websocket:
                    self.connections[connection_type] = websocket
                    connect_time = time.time()
                    self._update_status(connection_type, {
                        "connected": True,
                        "connecting": False,
                        "connected_at": connect_time,
                        "last_error": None,
                        "connection_url": str(websocket.url),
                        "connection_id": id(websocket),
                        "client_info": {
                            "local_address": websocket.local_address if hasattr(websocket, 'local_address') else None,
                            "remote_address": websocket.remote_address if hasattr(websocket, 'remote_address') else None,
                        }
                    })
                    logger.info(f"Successfully connected to {connection_type} WebSocket")
                    
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
                                callback(data)
                            except json.JSONDecodeError as e:
                                logger.error(f"Failed to parse WebSocket message: {str(e)}")
                                self._update_status(connection_type, {"last_error": f"Message parse error: {str(e)}"})
                    
                    except ConnectionClosed as e:
                        logger.warning(f"{connection_type} WebSocket connection closed: {str(e)}")
                        self._update_status(connection_type, {
                            "connected": False,
                            "last_error": f"Connection closed: {str(e)}"
                        })
                    
                    except Exception as e:
                        logger.error(f"Error in WebSocket message loop: {str(e)}")
                        self._update_status(connection_type, {
                            "connected": False,
                            "last_error": str(e)
                        })
            
            except InvalidStatusCode as e:
                logger.error(f"Invalid status code from WebSocket server: {str(e)}")
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": f"Invalid status code: {e.status_code}"
                })
            
            except InvalidMessage as e:
                logger.error(f"Invalid WebSocket message: {str(e)}")
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": "Invalid message format"
                })
            
            except ConnectionRefusedError as e:
                logger.error(f"WebSocket connection refused: {str(e)}")
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": "Connection refused"
                })
            
            except Exception as e:
                logger.error(f"Unexpected WebSocket error: {str(e)}")
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": str(e)
                })
            
            finally:
                self.connections.pop(connection_type, None)
            
            if not self.active:
                break
            
            retry_count += 1
            if max_retries and retry_count >= max_retries:
                logger.error(f"Maximum retry attempts ({max_retries}) reached for {connection_type} WebSocket")
                self._update_status(connection_type, {
                    "retry_count": retry_count,
                    "last_error": f"Maximum retries ({max_retries}) reached"
                })
                break
            
            # Exponential backoff with jitter
            delay = min(initial_retry_delay * (2 ** (retry_count - 1)), 30)  # Max 30 seconds
            jitter = delay * 0.1  # 10% jitter
            delay = delay + (asyncio.get_event_loop().time() % jitter)
            logger.debug(f"Retrying {connection_type} WebSocket connection in {delay:.2f} seconds")
            await asyncio.sleep(delay)
    
    async def disconnect(self, connection_type: Optional[str] = None):
        """Disconnect WebSocket connection(s)"""
        self.active = False
        if connection_type:
            websocket = self.connections.get(connection_type)
            if websocket:
                await websocket.close()
                self.connections.pop(connection_type, None)
                self._update_status(connection_type, {
                    "connected": False,
                    "last_error": "Disconnected by client"
                })
        else:
            for type_, websocket in self.connections.items():
                await websocket.close()
                self._update_status(type_, {
                    "connected": False,
                    "last_error": "Disconnected by client"
                })
            self.connections.clear()
