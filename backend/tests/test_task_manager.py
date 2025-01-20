import pytest
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch
import json
from app.core.task_manager import TaskManager
from app.api.schemas import CrawlerSettings, CrawlTask, DiscoveryTask

@pytest.mark.asyncio
class TestTaskManager:
    """Test task manager functionality"""

    async def test_initialization(self, mock_snowflake, websocket_manager):
        """Test manager initialization"""
        manager = TaskManager(mock_snowflake, websocket_manager)
        assert manager.snowflake == mock_snowflake
        assert manager.websocket_manager == websocket_manager
        assert isinstance(manager.active_tasks, dict)
        assert isinstance(manager.discovery_tasks, dict)
        assert manager._lock is not None

    async def test_create_task(self, task_manager):
        """Test crawl task creation"""
        urls = ["https://example.com"]
        settings = CrawlerSettings(batch_size=5)
        
        task = await task_manager.create_task(urls, settings)
        
        assert task.task_id in task_manager.active_tasks
        assert task.urls == urls
        assert task.settings == settings
        assert task.status == "created"
        assert task.progress == 0.0
        assert task.error is None
        assert isinstance(task.created_at, datetime)
        assert isinstance(task.updated_at, datetime)
        
        # Verify metrics initialization
        assert task.metrics["successful"] == 0
        assert task.metrics["failed"] == 0
        assert task.metrics["skipped"] == 0
        assert task.metrics["urls_per_second"] == 0.0

    async def test_discover_urls(self, task_manager, mock_snowflake):
        """Test URL discovery task"""
        url = "https://example.com"
        settings = CrawlerSettings(max_depth=2)
        
        task = await task_manager.discover_urls(url, "full", settings)
        
        assert task.task_id in task_manager.discovery_tasks
        assert task.start_url == url
        assert task.mode == "full"
        assert task.settings == settings
        assert task.status == "created"
        assert task.progress == 0.0
        assert not task.discovered_urls
        assert task.error is None

    async def test_run_discovery(self, task_manager, mock_snowflake, mocker):
        """Test URL discovery execution"""
        url = "https://example.com"
        task = await task_manager.discover_urls(url, "full")

        mock_result = {
            "urls": ["https://example.com/page1", "https://example.com/page2"],
            "total": 2,
            "max_depth": 1,
            "graph": {
                "nodes": [{"id": url, "depth": 0}],
                "links": []
            }
        }

        # Mock discovery manager
        with patch("app.core.url_discovery.URLDiscoveryManager.discover_urls", 
                  new_callable=AsyncMock, return_value=mock_result):
            await task_manager._run_discovery(task)

        assert task.status == "completed"
        assert task.progress == 1.0
        assert len(task.discovered_urls) == 2
        assert task.error is None
        mock_snowflake.save_discovery_result.assert_called_once()

    async def test_discovery_error(self, task_manager, mock_snowflake, mocker):
        """Test URL discovery error handling"""
        task = await task_manager.discover_urls("https://example.com", "full")
        
        # Mock discovery failure
        with patch("app.core.url_discovery.URLDiscoveryManager.discover_urls",
                  new_callable=AsyncMock, side_effect=Exception("Discovery failed")):
            await task_manager._run_discovery(task)

        assert task.status == "failed"
        assert "Discovery failed" in str(task.error)
        assert task.progress == 0.0

    async def test_start_task(self, task_manager, mock_snowflake, mocker):
        """Test starting crawl task"""
        task = await task_manager.create_task(["https://example.com"])
        task_id = task.task_id
        
        # Mock crawling process
        mocker.patch.object(task_manager, '_run_crawling', AsyncMock())
        
        started_task = await task_manager.start_task(task_id)
        assert started_task.status == "running"
        assert started_task.updated_at > started_task.created_at
        task_manager._run_crawling.assert_called_once_with(started_task)
        
        # Test invalid task ID
        with pytest.raises(ValueError):
            await task_manager.start_task("invalid_id")
        
        # Test already running task
        with pytest.raises(ValueError):
            await task_manager.start_task(task_id)

    async def test_crawl_execution(self, task_manager, mock_snowflake, mocker):
        """Test crawling execution"""
        urls = ["https://example.com/1", "https://example.com/2"]
        task = await task_manager.create_task(urls)

        # Mock crawler results
        mock_results = [
            Mock(
                url=url,
                success=True,
                html="<html></html>",
                error_message=None,
                media={},
                links={},
                metadata={}
            ) for url in urls
        ]

        # Mock crawler
        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = mock_results
        mocker.patch("crawl4ai.AsyncWebCrawler", return_value=mock_crawler)

        await task_manager._run_crawling(task)

        assert task.status == "completed"
        assert task.progress == 1.0
        assert task.metrics["successful"] == 2
        assert task.metrics["failed"] == 0
        assert task.error is None
        assert mock_snowflake.save_crawl_result.call_count == 2

    async def test_crawl_failure(self, task_manager, mock_snowflake, mocker):
        """Test crawling failures"""
        task = await task_manager.create_task(["https://example.com"])

        # Mock crawler with error
        mock_crawler = AsyncMock()
        mock_crawler.arun_many.side_effect = Exception("Crawl error")
        mocker.patch("crawl4ai.AsyncWebCrawler", return_value=mock_crawler)

        await task_manager._run_crawling(task)

        assert task.status == "failed"
        assert "Crawl error" in str(task.error)
        assert task.metrics["failed"] > 0
        assert mock_snowflake.save_crawl_result.call_count == 0

    async def test_partial_failures(self, task_manager, mock_snowflake, mocker):
        """Test handling of partial failures during crawling"""
        urls = ["https://example.com/1", "https://example.com/2"]
        task = await task_manager.create_task(urls)

        # Mock mixed results (success and failure)
        mock_results = [
            Mock(
                url=urls[0],
                success=True,
                html="<html></html>",
                error_message=None,
                media={},
                links={},
                metadata={}
            ),
            Mock(
                url=urls[1],
                success=False,
                html=None,
                error_message="Failed to load",
                media={},
                links={},
                metadata={}
            )
        ]

        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = mock_results
        mocker.patch("crawl4ai.AsyncWebCrawler", return_value=mock_crawler)

        await task_manager._run_crawling(task)

        assert task.status == "completed"
        assert task.metrics["successful"] == 1
        assert task.metrics["failed"] == 1
        assert mock_snowflake.save_crawl_result.call_count == 1

    async def test_cleanup(self, task_manager, mocker):
        """Test task cleanup"""
        task = await task_manager.create_task(["https://example.com"])
        
        # Mock async sleep
        mock_sleep = AsyncMock()
        mocker.patch("asyncio.sleep", mock_sleep)
        
        # Mock successful crawl
        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = []
        mocker.patch("crawl4ai.AsyncWebCrawler", return_value=mock_crawler)

        await task_manager._run_crawling(task)
        
        mock_sleep.assert_called_once_with(3600)  # 1 hour cleanup delay
        assert task.task_id not in task_manager.active_tasks

    async def test_update_task(self, task_manager, websocket_manager):
        """Test task updates"""
        task = await task_manager.create_task(["https://example.com"])
        
        # Test metrics update
        metrics_update = {
            "metrics": {
                "successful": 1,
                "failed": 0,
                "skipped": 0,
                "urls_per_second": 1.0
            }
        }
        updated = await task_manager.update_task(task.task_id, metrics_update)
        assert updated.metrics == metrics_update["metrics"]
        
        # Test progress update
        progress_update = {
            "progress": 0.5,
            "status": "running"
        }
        updated = await task_manager.update_task(task.task_id, progress_update)
        assert updated.progress == 0.5
        assert updated.status == "running"
        
        # Test invalid task
        assert await task_manager.update_task("invalid_id", {}) is None

    async def test_concurrent_operations(self, task_manager, mock_snowflake, mocker):
        """Test concurrent task operations"""
        # Create multiple tasks
        tasks = []
        for i in range(3):
            task = await task_manager.create_task([f"https://example.com/{i}"])
            tasks.append(task)
        
        # Mock crawler
        mock_crawler = AsyncMock()
        mock_crawler.arun_many.return_value = []
        mocker.patch("crawl4ai.AsyncWebCrawler", return_value=mock_crawler)
        
        # Run tasks concurrently
        await asyncio.gather(*(
            task_manager._run_crawling(task)
            for task in tasks
        ))
        
        # Verify all tasks completed
        assert all(task.status == "completed" for task in tasks)