import pytest
from fastapi import WebSocket, WebSocketDisconnect
from app.core.websocket_manager import WebSocketManager

@pytest.mark.asyncio
class TestWebSocketManager:
    """Test WebSocket manager functionality"""

    async def test_initialization(self):
        """Test manager initialization"""
        manager = WebSocketManager()
        assert manager.active_connections["metrics"] == set()
        assert manager.active_connections["progress"] == set()
        assert manager._lock is not None

    async def test_connect(self, mock_websocket):
        """Test WebSocket connection"""
        manager = WebSocketManager()
        
        # Test metrics connection
        await manager.connect("metrics", mock_websocket)
        assert mock_websocket in manager.active_connections["metrics"]
        assert len(manager.active_connections["metrics"]) == 1
        mock_websocket.accept.assert_called_once()

        # Test progress connection
        mock_websocket.accept.reset_mock()
        await manager.connect("progress", mock_websocket)
        assert mock_websocket in manager.active_connections["progress"]
        assert len(manager.active_connections["progress"]) == 1
        mock_websocket.accept.assert_called_once()

        # Test invalid connection type
        with pytest.raises(ValueError):
            await manager.connect("invalid", mock_websocket)

    async def test_disconnect(self, mock_websocket):
        """Test WebSocket disconnection"""
        manager = WebSocketManager()
        
        # Connect and then disconnect metrics
        await manager.connect("metrics", mock_websocket)
        await manager.disconnect("metrics", mock_websocket)
        assert mock_websocket not in manager.active_connections["metrics"]
        
        # Connect and then disconnect progress
        await manager.connect("progress", mock_websocket)
        await manager.disconnect("progress", mock_websocket)
        assert mock_websocket not in manager.active_connections["progress"]
        
        # Test disconnecting non-existent connection
        await manager.disconnect("metrics", mock_websocket)  # Should not raise error

    async def test_broadcast(self, mock_websocket):
        """Test message broadcasting"""
        manager = WebSocketManager()
        await manager.connect("metrics", mock_websocket)
        
        # Test successful broadcast
        test_message = {"status": "test"}
        await manager.broadcast("metrics", test_message)
        mock_websocket.send_json.assert_called_once_with(test_message)

        # Test broadcast with disconnected client
        mock_websocket.send_json.side_effect = WebSocketDisconnect()
        await manager.broadcast("metrics", test_message)
        assert len(manager.active_connections["metrics"]) == 0

    async def test_broadcast_metrics(self, mock_websocket):
        """Test metrics broadcast"""
        manager = WebSocketManager()
        await manager.connect("metrics", mock_websocket)
        
        metrics = {
            "successful": 10,
            "failed": 2,
            "skipped": 1
        }
        
        await manager.broadcast_metrics("task_123", metrics)
        mock_websocket.send_json.assert_called_once_with({
            "task_id": "task_123",
            "metrics": metrics
        })

    async def test_broadcast_progress(self, mock_websocket):
        """Test progress broadcast"""
        manager = WebSocketManager()
        await manager.connect("progress", mock_websocket)
        
        progress = {
            "progress": 0.5,
            "status": "running"
        }
        
        await manager.broadcast_progress("task_123", progress)
        mock_websocket.send_json.assert_called_once_with({
            "task_id": "task_123",
            "progress": progress
        })

    async def test_multiple_connections(self, mock_websocket):
        """Test handling multiple connections"""
        manager = WebSocketManager()
        
        # Create multiple mock websockets
        websockets = [mock_websocket] + [
            pytest.Mock(spec=WebSocket) for _ in range(2)
        ]
        for ws in websockets[1:]:
            ws.accept = pytest.AsyncMock()
            ws.send_json = pytest.AsyncMock()
        
        # Connect all websockets
        for ws in websockets:
            await manager.connect("metrics", ws)
        
        assert len(manager.active_connections["metrics"]) == 3
        
        # Broadcast to all
        test_message = {"test": "data"}
        await manager.broadcast("metrics", test_message)
        
        for ws in websockets:
            ws.send_json.assert_called_once_with(test_message)

    async def test_connection_cleanup(self, mock_websocket):
        """Test cleanup of failed connections"""
        manager = WebSocketManager()
        await manager.connect("metrics", mock_websocket)
        
        # Simulate failed connection
        mock_websocket.send_json.side_effect = Exception("Connection lost")
        
        # Broadcast should remove failed connection
        await manager.broadcast("metrics", {"test": "data"})
        assert len(manager.active_connections["metrics"]) == 0

    async def test_concurrent_operations(self, mock_websocket):
        """Test concurrent operations"""
        manager = WebSocketManager()
        
        # Create multiple concurrent tasks
        async def connect_and_broadcast():
            await manager.connect("metrics", mock_websocket)
            await manager.broadcast_metrics("task", {"count": 1})
        
        tasks = [connect_and_broadcast() for _ in range(5)]
        await asyncio.gather(*tasks)
        
        assert len(manager.active_connections["metrics"]) == 5
        assert mock_websocket.send_json.call_count == 5

    async def test_get_connection_count(self, mock_websocket):
        """Test connection counting"""
        manager = WebSocketManager()
        
        assert manager.get_connection_count("metrics") == 0
        
        await manager.connect("metrics", mock_websocket)
        assert manager.get_connection_count("metrics") == 1
        
        await manager.disconnect("metrics", mock_websocket)
        assert manager.get_connection_count("metrics") == 0
        
        # Test invalid connection type
        assert manager.get_connection_count("invalid") == 0