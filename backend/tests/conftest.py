import pytest
import asyncio
from typing import AsyncGenerator, Generator
from fastapi.testclient import TestClient
from pytest_mock import MockFixture

from app.main import app
from app.core.config import settings
from app.database.snowflake_manager import SnowflakeManager
from app.core.websocket_manager import WebSocketManager
from app.core.task_manager import TaskManager
from app.core.storage_manager import StorageManager

@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
async def client() -> AsyncGenerator:
    """Get FastAPI test client"""
    async with TestClient(app) as client:
        yield client

@pytest.fixture
async def mock_snowflake(mocker: MockFixture) -> SnowflakeManager:
    """Create mocked Snowflake manager"""
    mock = mocker.Mock(spec=SnowflakeManager)
    
    # Mock connection methods
    mock.get_connection.return_value = mocker.Mock()
    mock.execute_query.return_value = []
    
    # Mock success responses for save operations
    mock.save_crawl_result.return_value = True
    mock.save_discovery_result.return_value = True
    mock.save_file_info.return_value = True
    mock.save_task_metrics.return_value = True
    
    return mock

@pytest.fixture
async def websocket_manager() -> WebSocketManager:
    """Create WebSocket manager instance"""
    return WebSocketManager()

@pytest.fixture
async def storage_manager(mock_snowflake: SnowflakeManager) -> StorageManager:
    """Create storage manager instance"""
    return StorageManager(mock_snowflake)

@pytest.fixture
async def task_manager(
    mock_snowflake: SnowflakeManager,
    websocket_manager: WebSocketManager
) -> TaskManager:
    """Create task manager instance"""
    return TaskManager(mock_snowflake, websocket_manager)

@pytest.fixture
def test_settings():
    """Test settings"""
    return {
        "SNOWFLAKE_ACCOUNT": "test_account",
        "SNOWFLAKE_USER": "test_user",
        "SNOWFLAKE_PASSWORD": "test_password",
        "SNOWFLAKE_DATABASE": "test_db",
        "SNOWFLAKE_SCHEMA": "test_schema",
        "SNOWFLAKE_WAREHOUSE": "test_wh",
        "SNOWFLAKE_ROLE": "test_role"
    }

@pytest.fixture
def sample_crawl_result():
    """Sample crawl result data"""
    return {
        "url": "https://example.com",
        "success": True,
        "html": "<html><body>Test content</body></html>",
        "cleaned_html": "<body>Test content</body>",
        "error_message": None,
        "media": {"images": [], "pdfs": []},
        "links": {"internal": [], "external": []},
        "metadata": {"title": "Test Page"}
    }

@pytest.fixture
def sample_discovery_result():
    """Sample URL discovery result data"""
    return {
        "task_id": "test_task_123",
        "start_url": "https://example.com",
        "discovered_urls": [
            "https://example.com/page1",
            "https://example.com/page2"
        ],
        "total_urls": 2,
        "max_depth": 1,
        "url_graph": {
            "nodes": [
                {"id": "https://example.com", "depth": 0},
                {"id": "https://example.com/page1", "depth": 1},
                {"id": "https://example.com/page2", "depth": 1}
            ],
            "links": [
                {"source": "https://example.com", "target": "https://example.com/page1"},
                {"source": "https://example.com", "target": "https://example.com/page2"}
            ]
        },
        "created_at": "2024-01-20T10:00:00",
        "completed_at": "2024-01-20T10:01:00"
    }

@pytest.fixture
def mock_websocket(mocker: MockFixture):
    """Create mock WebSocket connection"""
    mock = mocker.Mock()
    mock.accept = mocker.AsyncMock()
    mock.send_json = mocker.AsyncMock()
    mock.send_text = mocker.AsyncMock()
    mock.receive_json = mocker.AsyncMock()
    mock.receive_text = mocker.AsyncMock()
    mock.close = mocker.AsyncMock()
    return mock

@pytest.fixture
def mock_crawler(mocker: MockFixture):
    """Create mock AsyncWebCrawler"""
    mock = mocker.AsyncMock()
    mock.__aenter__ = mocker.AsyncMock(return_value=mock)
    mock.__aexit__ = mocker.AsyncMock()
    mock.arun = mocker.AsyncMock()
    mock.arun_many = mocker.AsyncMock()
    return mock