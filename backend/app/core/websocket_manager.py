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
        if connection_type not in self.active_connections:
            raise ValueError(f"Invalid connection type: {connection_type}")
            
        await websocket.accept()
        async with self._lock:
            self.active_connections[connection_type].add(websocket)
        logger.info(f"New {connection_type} WebSocket connection")
    
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