import pytest
import json
from datetime import datetime
from unittest.mock import AsyncMock, Mock
from app.database.snowflake_manager import SnowflakeManager

@pytest.mark.asyncio
class TestSnowflakeManager:
    """Test Snowflake manager functionality"""

    async def test_initialization(self, test_settings):
        """Test manager initialization"""
        manager = SnowflakeManager()
        assert manager.config["user"] == test_settings["SNOWFLAKE_USER"]
        assert manager.config["database"] == test_settings["SNOWFLAKE_DATABASE"]
        assert manager._conn is None
        assert manager._lock is not None

    async def test_get_connection(self, mocker):
        """Test connection creation and reuse"""
        # Mock snowflake.connector.connect
        mock_connect = mocker.patch("snowflake.connector.connect")
        mock_conn = Mock()
        mock_connect.return_value = mock_conn

        manager = SnowflakeManager()
        
        # First connection
        conn1 = await manager.get_connection()
        assert conn1 == mock_conn
        assert manager._conn == mock_conn
        mock_connect.assert_called_once()

        # Second connection should reuse existing
        conn2 = await manager.get_connection()
        assert conn2 == conn1
        mock_connect.assert_called_once()

    async def test_execute_query_with_fetch(self, mocker):
        """Test query execution with result fetching"""
        manager = SnowflakeManager()
        mock_cursor = Mock()
        mock_cursor.description = [("col1",), ("col2",)]
        mock_cursor.fetchall = Mock(return_value=[(1, "a"), (2, "b")])
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mocker.patch.object(manager, "get_connection", AsyncMock(return_value=mock_conn))

        results = await manager.execute_query(
            "SELECT col1, col2 FROM test",
            {"param": "value"}
        )

        assert len(results) == 2
        assert results[0] == {"col1": 1, "col2": "a"}
        assert results[1] == {"col1": 2, "col2": "b"}
        mock_cursor.execute.assert_called_once_with(
            "SELECT col1, col2 FROM test",
            {"param": "value"}
        )

    async def test_save_discovery_result(self, mocker):
        """Test saving discovery results"""
        manager = SnowflakeManager()
        mocker.patch.object(manager, "execute_query", AsyncMock(return_value=None))

        discovery_result = {
            "task_id": "test_task",
            "start_url": "https://example.com",
            "discovered_urls": ["url1", "url2"],
            "total_urls": 2,
            "max_depth": 1,
            "url_graph": {"nodes": [], "links": []},
            "created_at": datetime.now(),
            "completed_at": datetime.now()
        }

        success = await manager.save_discovery_result(discovery_result)
        assert success is True
        manager.execute_query.assert_called_once()
        call_args = manager.execute_query.call_args[0]
        assert "INSERT INTO discovery_results" in call_args[0]
        assert call_args[1]["task_id"] == discovery_result["task_id"]

    async def test_save_crawl_result(self, mocker):
        """Test saving crawl results"""
        manager = SnowflakeManager()
        mocker.patch.object(manager, "execute_query", AsyncMock(return_value=None))

        crawl_result = {
            "url": "https://example.com",
            "success": True,
            "html": "<html>content</html>",
            "cleaned_html": "<div>content</div>",
            "error_message": None,
            "media": {"images": []},
            "links": {"internal": []},
            "metadata": {"title": "Test"}
        }

        success = await manager.save_crawl_result(crawl_result)
        assert success is True
        manager.execute_query.assert_called_once()
        call_args = manager.execute_query.call_args[0]
        assert "INSERT INTO crawl_results" in call_args[0]
        assert call_args[1]["url"] == crawl_result["url"]

    async def test_get_crawl_result(self, mocker):
        """Test retrieving crawl results"""
        manager = SnowflakeManager()
        mock_result = {
            "url": "https://example.com",
            "success": True,
            "media_data": json.dumps({"images": []}),
            "links_data": json.dumps({"internal": []}),
            "metadata": json.dumps({"title": "Test"})
        }
        mocker.patch.object(manager, "execute_query", AsyncMock(return_value=[mock_result]))

        result = await manager.get_crawl_result("https://example.com")
        assert result is not None
        assert result["url"] == "https://example.com"
        assert isinstance(result["media_data"], dict)
        assert isinstance(result["links_data"], dict)
        assert isinstance(result["metadata"], dict)

    async def test_save_file_info(self, mocker):
        """Test saving file information"""
        manager = SnowflakeManager()
        mocker.patch.object(manager, "execute_query", AsyncMock(return_value=None))

        file_info = {
            "url": "https://example.com",
            "file_type": "pdf",
            "stage_path": "@stage/path",
            "content_type": "application/pdf",
            "size": 1024,
            "metadata": {"pages": 5}
        }

        success = await manager.save_file_info(file_info)
        assert success is True
        manager.execute_query.assert_called_once()
        call_args = manager.execute_query.call_args[0]
        assert "INSERT INTO saved_files" in call_args[0]
        assert call_args[1]["url"] == file_info["url"]

    async def test_save_task_metrics(self, mocker):
        """Test saving task metrics"""
        manager = SnowflakeManager()
        mocker.patch.object(manager, "execute_query", AsyncMock(return_value=None))

        metrics = {
            "successful": 10,
            "failed": 2,
            "skipped": 1,
            "urls_per_second": 2.5
        }

        success = await manager.save_task_metrics("test_task", metrics)
        assert success is True
        manager.execute_query.assert_called_once()
        call_args = manager.execute_query.call_args[0]
        assert "INSERT INTO task_metrics" in call_args[0]
        assert call_args[1]["task_id"] == "test_task"

    async def test_get_stats(self, mocker):
        """Test retrieving crawler statistics"""
        manager = SnowflakeManager()
        
        mock_crawl_stats = [{
            "total_urls": 100,
            "successful_urls": 90,
            "failed_urls": 10
        }]
        
        mock_discovery_stats = [{
            "total_tasks": 5,
            "total_discovered_urls": 200,
            "avg_depth": 2.5
        }]
        
        mock_file_stats = [
            {"file_type": "pdf", "count": 10, "total_size": 1024},
            {"file_type": "image", "count": 20, "total_size": 2048}
        ]

        # Mock multiple execute_query calls
        mocker.patch.object(
            manager,
            "execute_query",
            AsyncMock(side_effect=[
                mock_crawl_stats,
                mock_discovery_stats,
                mock_file_stats
            ])
        )

        stats = await manager.get_stats()
        assert stats["crawl_stats"]["total_urls"] == 100
        assert stats["discovery_stats"]["total_tasks"] == 5
        assert len(stats["file_stats"]) == 2
        assert "last_update" in stats

    async def test_error_handling(self, mocker):
        """Test error handling in operations"""
        manager = SnowflakeManager()
        mocker.patch.object(
            manager,
            "execute_query",
            AsyncMock(side_effect=Exception("Database error"))
        )

        # Test error in save operations
        assert await manager.save_crawl_result({"url": "test"}) is False
        assert await manager.save_discovery_result({"task_id": "test"}) is False
        assert await manager.save_file_info({"url": "test"}) is False
        assert await manager.save_task_metrics("test", {}) is False

        # Test error in get operations
        assert await manager.get_crawl_result("test") is None
        assert await manager.get_discovery_result("test") is None
        
        # Test error in stats
        stats = await manager.get_stats()
        assert stats == {}