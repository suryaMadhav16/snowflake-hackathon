from fastapi import WebSocket
from typing import Dict, Set, Any
import logging
import asyncio

logger = logging.getLogger(__name__)

class WebSocketManager:
    """Manages WebSocket connections and updates"""
    
    def __init__(self):
        self.active_connections: Dict[str, Set[WebSocket]] = {
            'metrics': set(),
            'progress': set()
        }
        self._lock = asyncio.Lock()
    
    async def connect(self, connection_type: str, websocket: WebSocket):
        """Connect a WebSocket client"""
        logger.debug(f"Attempting to connect {connection_type} WebSocket from {websocket.client.host}")
        if connection_type not in self.active_connections:
            raise ValueError(f"Invalid connection type: {connection_type}")
        
        # Clean up any existing connections of this type
        await self.cleanup_connections(connection_type)
        
        await websocket.accept()
        logger.debug(f"WebSocket accepted for {connection_type}")
        async with self._lock:
            self.active_connections[connection_type].add(websocket)
            
        logger.info(f"New {connection_type} WebSocket connection. Total connections: {self.get_connection_count(connection_type)}")
        
        # Send initial connection confirmation
        try:
            await websocket.send_json({
                "status": "connected",
                "type": connection_type,
                "connection_id": id(websocket),
                "message": "WebSocket connection established"
            })
        except Exception as e:
            logger.error(f"Error sending connection confirmation: {str(e)}")
            
    async def cleanup_connections(self, connection_type: str):
        """Clean up existing connections of a type"""
        try:
            async with self._lock:
                connections = self.active_connections.get(connection_type, set()).copy()
                for websocket in connections:
                    try:
                        await websocket.close()
                    except Exception as e:
                        logger.error(f"Error closing websocket: {str(e)}")
                self.active_connections[connection_type].clear()
        except Exception as e:
            logger.error(f"Error cleaning up connections: {str(e)}")
    
    async def disconnect(self, connection_type: str, websocket: WebSocket):
        """Disconnect a WebSocket client"""
        async with self._lock:
            try:
                self.active_connections[connection_type].remove(websocket)
                logger.info(f"Removed {connection_type} WebSocket connection")
            except KeyError:
                pass
    
    async def broadcast(self, connection_type: str, message: Any):
        """Broadcast message to all connected clients of a type"""
        if not self.active_connections.get(connection_type):
            return
            
        disconnected = set()
        for connection in self.active_connections[connection_type]:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error(f"Error broadcasting to WebSocket: {str(e)}")
                disconnected.add(connection)
        
        # Clean up disconnected clients
        if disconnected:
            async with self._lock:
                self.active_connections[connection_type] -= disconnected
    
    async def broadcast_metrics(self, task_id: str, metrics: Dict):
        """Broadcast metrics update"""
        await self.broadcast('metrics', {
            'task_id': task_id,
            'metrics': metrics
        })
    
    async def broadcast_progress(self, task_id: str, progress: Dict):
        """Broadcast progress update"""
        await self.broadcast('progress', {
            'task_id': task_id,
            'progress': progress
        })
    
    def get_connection_count(self, connection_type: str) -> int:
        """Get count of active connections of a type"""
        return len(self.active_connections.get(connection_type, set()))
        
    def get_connection_status(self) -> dict:
        """Get detailed status of all connections"""
        return {
            conn_type: {
                "count": len(conns),
                "connections": [
                    {
                        "id": id(ws),
                        "client_host": ws.client.host if hasattr(ws.client, 'host') else 'unknown',
                        "connected_at": getattr(ws, 'connected_at', None)
                    } for ws in conns
                ]
            }
            for conn_type, conns in self.active_connections.items()
        }