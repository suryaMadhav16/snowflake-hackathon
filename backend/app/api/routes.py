from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, List, Optional
import logging
from pydantic import BaseModel
from datetime import datetime

from core.config import settings
from database.snowflake_manager import SnowflakeManager
from core.task_manager import TaskManager
from core.storage_manager import StorageManager
from api.schemas import CrawlerSettings, CrawlTask, CrawlResult

logger = logging.getLogger(__name__)
router = APIRouter()

class DiscoveryRequest(BaseModel):
    url: str
    mode: str = "full"
    settings: Optional[CrawlerSettings] = None

# Dependencies
async def get_snowflake():
    """Get Snowflake manager instance"""
    return SnowflakeManager()

async def get_task_manager(
    snowflake: SnowflakeManager = Depends(get_snowflake)
) -> TaskManager:
    """Get task manager instance"""
    return TaskManager(snowflake)

async def get_storage_manager(
    snowflake: SnowflakeManager = Depends(get_snowflake)
) -> StorageManager:
    """Get storage manager instance"""
    return StorageManager(snowflake)

# Core Routes
@router.post("/discover")
async def discover_urls(
    request: DiscoveryRequest,
    task_manager: TaskManager = Depends(get_task_manager)
) -> Dict:
    """Start URL discovery process"""
    try:
        task = await task_manager.discover_urls(request.url, request.mode, request.settings)
        logger.info(f"==Discovery task started==: {task.task_id}")
        return {
            "task_id": task.task_id,
            "status": task.status,
            "message": "URL discovery started"
        }
    except Exception as e:
        logger.error(f"URL discovery error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/crawl")
async def start_crawling(
    urls: List[str],
    settings: Optional[CrawlerSettings] = None,
    task_manager: TaskManager = Depends(get_task_manager)
) -> Dict:
    """Start crawling process"""
    try:
        task = await task_manager.create_task(urls, settings)
        started_task = await task_manager.start_task(task.task_id)
        return {
            "task_id": started_task.task_id,
            "status": started_task.status,
            "message": "Crawling started"
        }
    except Exception as e:
        logger.error(f"Crawling error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status/{task_id}")
async def get_status(
    task_id: str,
    task_manager: TaskManager = Depends(get_task_manager)
) -> Dict:
    """Get current task status"""
    try:
        # Check crawl task
        logger.debug(f"Checking status for task_id: {task_id}")
        task = await task_manager.get_task(task_id)
        if task:
            return {
                "task_id": task.task_id,
                "type": "crawl",
                "status": task.status,
                "progress": task.progress,
                "metrics": task.metrics,
                "error": task.error,
                "current_url": getattr(task, 'current_url', None),
                "updated_at": task.updated_at.isoformat()
            }
            
        # Check discovery task
        task = await task_manager.get_discovery_task(task_id)
        if task:
            return {
                "task_id": task.task_id,
                "type": "discovery",
                "status": task.status,
                "progress": task.progress,
                "total_urls": len(task.discovered_urls) if task.discovered_urls else 0,
                "current_url": getattr(task, 'current_url', None),
                "error": task.error,
                "updated_at": task.updated_at.isoformat()
            }
            
        raise HTTPException(status_code=404, detail="Task not found")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Status check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/results/{url}")
async def get_results(
    url: str,
    db: SnowflakeManager = Depends(get_snowflake)
) -> Dict:
    """Get crawling results for URL"""
    try:
        result = await db.get_crawl_result(url)
        if not result:
            raise HTTPException(status_code=404, detail="Result not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/discovery/{task_id}")
async def get_discovery_results(
    task_id: str,
    db: SnowflakeManager = Depends(get_snowflake)
) -> Dict:
    """Get URL discovery results"""
    try:
        result = await db.get_discovery_result(task_id)
        if not result:
            raise HTTPException(status_code=404, detail="Discovery result not found")
        return result
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting discovery results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/files/{url}")
async def get_files(
    url: str,
    file_type: Optional[str] = None,
    storage: StorageManager = Depends(get_storage_manager)
) -> List[Dict]:
    """Get saved files for URL"""
    try:
        files = await storage.list_files(url, file_type)
        return files
    except Exception as e:
        logger.error(f"File retrieval error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/file-content/{url}")
async def get_file_content(
    url: str,
    file_type: str,
    storage: StorageManager = Depends(get_storage_manager)
) -> Dict:
    """Get file content from storage"""
    try:
        content = await storage.get_file(url, file_type)
        if not content:
            raise HTTPException(status_code=404, detail="File not found")
        
        return {
            "url": url,
            "file_type": file_type,
            "content": content
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting file content: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/stats")
async def get_stats(
    db: SnowflakeManager = Depends(get_snowflake)
) -> Dict:
    """Get crawler statistics"""
    try:
        return await db.get_stats()
    except Exception as e:
        logger.error(f"Stats error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))