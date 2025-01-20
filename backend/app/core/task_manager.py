import asyncio
import uuid
import logging
from typing import Dict, List, Optional, Set
from datetime import datetime
from .websocket_manager import WebSocketManager
from .url_discovery import URLDiscoveryManager
from ..database.snowflake_manager import SnowflakeManager
from ..api.schemas import CrawlerSettings, CrawlTask, DiscoveryTask
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logger = logging.getLogger(__name__)

class TaskManager:
    """Manages crawling tasks and their state"""
    
    def __init__(
        self,
        snowflake: SnowflakeManager,
        websocket_manager: WebSocketManager
    ):
        self.snowflake = snowflake
        self.websocket_manager = websocket_manager
        self.active_tasks: Dict[str, CrawlTask] = {}
        self.discovery_tasks: Dict[str, DiscoveryTask] = {}
        self._lock = asyncio.Lock()
    
    def generate_task_id(self) -> str:
        """Generate unique task ID"""
        return str(uuid.uuid4())
    
    async def discover_urls(
        self,
        url: str,
        mode: str = "full",
        settings: Optional[CrawlerSettings] = None
    ) -> DiscoveryTask:
        """Start URL discovery process"""
        task_id = self.generate_task_id()
        settings = settings or CrawlerSettings()
        
        task = DiscoveryTask(
            task_id=task_id,
            start_url=url,
            mode=mode,
            settings=settings,
            status="created",
            progress=0.0,
            discovered_urls=[],
            error=None,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        self.discovery_tasks[task_id] = task
        
        # Start discovery in background
        asyncio.create_task(self._run_discovery(task))
        
        return task
    
    async def _run_discovery(self, task: DiscoveryTask):
        """Run URL discovery process"""
        try:
            task.status = "running"
            task.updated_at = datetime.now()
            
            # Create URL discovery manager
            discovery_manager = URLDiscoveryManager(
                max_depth=task.settings.max_depth,
                excluded_patterns=task.settings.exclusion_patterns
            )
            
            # Run discovery
            result = await discovery_manager.discover_urls(
                task.start_url,
                task.mode
            )
            
            # Update task with results
            task.discovered_urls = result["urls"]
            task.total_urls = result["total"]
            task.max_depth = result["max_depth"]
            task.url_graph = result["graph"]
            task.status = "completed"
            task.progress = 1.0
            task.updated_at = datetime.now()
            
            # Save discovery results to Snowflake
            try:
                await self.snowflake.save_discovery_result({
                    "task_id": task.task_id,
                    "start_url": task.start_url,
                    "discovered_urls": task.discovered_urls,
                    "total_urls": task.total_urls,
                    "max_depth": task.max_depth,
                    "url_graph": task.url_graph,
                    "created_at": task.created_at,
                    "completed_at": task.updated_at
                })
            except Exception as e:
                logger.error(f"Error saving discovery results: {str(e)}")
            
            # Broadcast completion
            await self.websocket_manager.broadcast_progress(
                task.task_id,
                {
                    'progress': 1.0,
                    'status': 'completed',
                    'total_urls': task.total_urls
                }
            )
            
        except Exception as e:
            error_message = f"Discovery error: {str(e)}"
            logger.error(error_message)
            task.status = "failed"
            task.error = error_message
            task.updated_at = datetime.now()
            
            await self.websocket_manager.broadcast_progress(
                task.task_id,
                {
                    'status': 'failed',
                    'error': error_message
                }
            )
        
        finally:
            # Clean up task after some time
            await asyncio.sleep(3600)  # Keep task info for 1 hour
            self.discovery_tasks.pop(task.task_id, None)
    
    async def create_task(
        self,
        urls: List[str],
        settings: Optional[CrawlerSettings] = None
    ) -> CrawlTask:
        """Create new crawling task"""
        task_id = self.generate_task_id()
        settings = settings or CrawlerSettings()
        
        task = CrawlTask(
            task_id=task_id,
            urls=urls,
            settings=settings,
            status="created",
            progress=0.0,
            metrics={
                "successful": 0,
                "failed": 0,
                "skipped": 0,
                "urls_per_second": 0.0,
                "memory_usage": 0,
                "batch_processing_time": 0.0,
                "total_processing_time": 0.0
            },
            error=None,
            created_at=datetime.now(),
            updated_at=datetime.now()
        )
        
        async with self._lock:
            self.active_tasks[task_id] = task
        
        return task
    
    async def start_task(self, task_id: str):
        """Start a crawling task"""
        task = await self.get_task(task_id)
        if not task:
            raise ValueError(f"Task not found: {task_id}")
        
        if task.status != "created":
            raise ValueError(f"Task {task_id} is already {task.status}")
        
        # Update task status
        task.status = "running"
        task.updated_at = datetime.now()
        
        # Start crawling in background
        asyncio.create_task(self._run_crawling(task))
        
        return task
    
    async def get_task(self, task_id: str) -> Optional[CrawlTask]:
        """Get task by ID"""
        return self.active_tasks.get(task_id)
    
    async def get_discovery_task(self, task_id: str) -> Optional[DiscoveryTask]:
        """Get discovery task by ID"""
        return self.discovery_tasks.get(task_id)
    
    async def update_task(
        self,
        task_id: str,
        updates: Dict
    ) -> Optional[CrawlTask]:
        """Update task attributes"""
        task = await self.get_task(task_id)
        if not task:
            return None
        
        async with self._lock:
            for key, value in updates.items():
                if hasattr(task, key):
                    setattr(task, key, value)
            task.updated_at = datetime.now()
            
            # Broadcast updates
            if 'metrics' in updates:
                await self.websocket_manager.broadcast_metrics(
                    task_id,
                    task.metrics
                )
            
            if 'progress' in updates:
                await self.websocket_manager.broadcast_progress(
                    task_id,
                    {
                        'progress': task.progress,
                        'status': task.status
                    }
                )
        
        return task
    
    async def _run_crawling(self, task: CrawlTask):
        """Run crawling process"""
        try:
            total_urls = len(task.urls)
            
            # Configure crawler
            browser_config = BrowserConfig(
                headless=True,
                browser_type=task.settings.browser_type,
                user_agent_mode="random",
                viewport_width=1080,
                viewport_height=800
            )
            
            crawler_config = CrawlerRunConfig(
                magic=True,
                simulate_user=True,
                cache_mode=CacheMode.ENABLED,
                mean_delay=1.0,
                max_range=0.3,
                semaphore_count=5,
                screenshot=task.settings.capture_screenshots,
                pdf=task.settings.generate_pdfs
            )
            
            start_time = datetime.now()
            processed_urls = 0
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                # Process URLs in batches
                for i in range(0, total_urls, task.settings.batch_size):
                    batch = task.urls[i:i + task.settings.batch_size]
                    batch_start = datetime.now()
                    
                    try:
                        results = await crawler.arun_many(
                            urls=batch,
                            config=crawler_config
                        )
                        
                        # Process results
                        for result in results:
                            try:
                                if result.success:
                                    task.metrics["successful"] += 1
                                    # Save to Snowflake
                                    await self.snowflake.save_crawl_result({
                                        "url": result.url,
                                        "success": result.success,
                                        "html": result.html,
                                        "cleaned_html": getattr(result, 'cleaned_html', None),
                                        "error_message": result.error_message,
                                        "media": result.media,
                                        "links": result.links,
                                        "metadata": getattr(result, 'metadata', {})
                                    })
                                else:
                                    task.metrics["failed"] += 1
                                    logger.warning(
                                        f"Crawl failed for {result.url}: {result.error_message}"
                                    )
                            except Exception as e:
                                task.metrics["failed"] += 1
                                logger.error(
                                    f"Error processing result for {result.url}: {str(e)}"
                                )
                        
                        processed_urls += len(batch)
                        
                        # Update metrics
                        batch_duration = (datetime.now() - batch_start).total_seconds()
                        total_duration = (datetime.now() - start_time).total_seconds()
                        
                        await self.update_task(task.task_id, {
                            "progress": processed_urls / total_urls,
                            "metrics": {
                                **task.metrics,
                                "urls_per_second": processed_urls / total_duration if total_duration > 0 else 0,
                                "batch_processing_time": batch_duration,
                                "total_processing_time": total_duration
                            }
                        })
                        
                    except Exception as e:
                        logger.error(f"Batch processing error: {str(e)}")
                        task.metrics["failed"] += len(batch)
            
            # Update final status
            await self.update_task(task.task_id, {
                "status": "completed",
                "progress": 1.0,
                "metrics": task.metrics
            })
            
        except Exception as e:
            error_message = f"Crawling error: {str(e)}"
            logger.error(error_message)
            await self.update_task(task.task_id, {
                "status": "failed",
                "error": error_message
            })
        
        finally:
            # Clean up task after some time
            await asyncio.sleep(3600)  # Keep task info for 1 hour
            async with self._lock:
                self.active_tasks.pop(task.task_id, None)