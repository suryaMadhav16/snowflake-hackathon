import asyncio
import uuid
import logging
import json
from typing import Dict, List, Optional, Set
from datetime import datetime
from core.url_discovery import URLDiscoveryManager
from database.snowflake_manager import SnowflakeManager
from core.storage_manager import StorageManager
from api.schemas import CrawlerSettings, CrawlTask, DiscoveryTask, TaskMetrics, URLGraph
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logger = logging.getLogger(__name__)

class TaskManager:
    """Manages crawling tasks and their state"""
    
    def __init__(
        self,
        snowflake: SnowflakeManager
    ):
        self.snowflake = snowflake
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
        
        # Save task to database
        await self.snowflake.save_task({
            'task_id': task.task_id,
            'task_type': 'discovery',
            'status': task.status,
            'progress': task.progress,
            'settings': task.settings.dict() if task.settings else {},
            'error': task.error,
            'current_url': task.start_url,
            'urls': [task.start_url],
            'created_at': task.created_at,
            'updated_at': task.updated_at
        })
        
        # Start discovery in background
        asyncio.create_task(self._run_discovery(task))
        
        return task
    
    async def _run_discovery(self, task: DiscoveryTask):
        """Run URL discovery process"""
        try:
            task.status = "running"
            task.updated_at = datetime.now()
            
            # Update task in database
            await self.snowflake.save_task({
                'task_id': task.task_id,
                'task_type': 'discovery',
                'status': task.status,
                'progress': task.progress,
                'settings': task.settings.dict() if task.settings else {},
                'error': task.error,
                'current_url': task.start_url,
                'urls': [task.start_url],
                'created_at': task.created_at,
                'updated_at': task.updated_at
            })
            
            # Create URL discovery manager
            discovery_manager = URLDiscoveryManager(
                max_depth=task.settings.max_depth,
                excluded_patterns=task.settings.exclusion_patterns
            )
            
            # Run discovery
            start_time = datetime.now()
            
            async def update_progress(current_url: str, progress: float):
                task.current_url = current_url
                task.progress = progress
                elapsed_time = (datetime.now() - start_time).total_seconds()
                
                await self.snowflake.save_task({
                    'task_id': task.task_id,
                    'task_type': 'discovery',
                    'status': task.status,
                    'progress': task.progress,
                    'settings': task.settings.dict() if task.settings else {},
                    'error': task.error,
                    'current_url': current_url,
                    'urls': [task.start_url],
                    'created_at': task.created_at,
                    'updated_at': datetime.now(),
                    'metrics': {
                        'elapsed_time': elapsed_time,
                        'current_url': current_url
                    }
                })

            result = await discovery_manager.discover_urls(
                task.start_url,
                task.mode,
                progress_callback=update_progress
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
                r = await self.snowflake.get_discovery_result(task.task_id)
                logger.info(f"======Discovery result======: {r}")
            except Exception as e:
                logger.error(f"Error saving discovery results: {str(e)}")
            
        except Exception as e:
            error_message = f"Discovery error: {str(e)}"
            logger.error(error_message)
            task.status = "failed"
            task.error = error_message
            task.updated_at = datetime.now()
        
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
                "total_processing_time": 0.0,
                "saved_content": {
                    "markdown": 0,
                    "images": 0,
                    "pdf": 0,
                    "screenshot": 0
                }
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
        """Get task by ID from memory or database"""
        # First check memory        
        task = self.active_tasks.get(task_id)
        if task:
            return task

        # Then check database
        db_task = await self.snowflake.get_task(task_id)
        if db_task:
            try:
                # Case insensitive access
                def get_field(field_name: str):
                    # Try lowercase first
                    if field_name in db_task:
                        return db_task[field_name]
                    # Then try uppercase
                    return db_task[field_name.upper()]

                # Parse URLs from JSON string
                urls_str = get_field('urls')
                urls = json.loads(urls_str) if isinstance(urls_str, str) else urls_str

                # Parse settings from JSON string
                settings_str = get_field('settings')
                settings_dict = json.loads(settings_str) if isinstance(settings_str, str) else settings_str
                settings = CrawlerSettings(**settings_dict)

                # Parse metrics from JSON string
                metrics_str = get_field('metrics')
                metrics_dict = json.loads(metrics_str) if isinstance(metrics_str, str) else metrics_str
                metrics = TaskMetrics(**metrics_dict)

                task = CrawlTask(
                    task_id=get_field('task_id'),
                    urls=urls,
                    settings=settings,
                    status=get_field('status'),
                    progress=get_field('progress'),
                    metrics=metrics,
                    error=get_field('error'),
                    created_at=get_field('created_at'),
                    updated_at=get_field('updated_at')
                )
                return task
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from database: {str(e)}")
            except Exception as e:
                logger.error(f"Error creating task from database: {str(e)}")

        return None
    
    async def get_discovery_task(self, task_id: str) -> Optional[DiscoveryTask]:
        """Get discovery task by ID from memory or database"""
        # First check memory
        task = self.discovery_tasks.get(task_id)
        if task:
            return task

        # Then check database
        db_task = await self.snowflake.get_task(task_id)
        if db_task and db_task.get('TASK_TYPE', '').lower() == 'discovery':
            try:
                # Case insensitive access
                def get_field(field_name: str, task_dict=db_task):
                    # Try lowercase first
                    if field_name in task_dict:
                        return task_dict[field_name]
                    # Then try uppercase
                    return task_dict[field_name.upper()]

                discovery_result = await self.snowflake.get_discovery_result(task_id)
                if discovery_result:
                    # Parse settings
                    settings_str = get_field('settings')
                    settings_dict = json.loads(settings_str) if isinstance(settings_str, str) else settings_str
                    settings = CrawlerSettings(**settings_dict)

                    # Parse discovered URLs
                    urls_str = get_field('discovered_urls', discovery_result)
                    discovered_urls = json.loads(urls_str) if isinstance(urls_str, str) else urls_str

                    # Parse URL graph
                    graph_str = get_field('url_graph', discovery_result)
                    graph_dict = json.loads(graph_str) if isinstance(graph_str, str) else graph_str
                    url_graph = URLGraph(**graph_dict) if graph_dict else None

                    task = DiscoveryTask(
                        task_id=get_field('task_id'),
                        start_url=get_field('start_url', discovery_result),
                        mode=settings_dict.get('mode', 'full'),
                        settings=settings,
                        status=get_field('status'),
                        progress=get_field('progress'),
                        discovered_urls=discovered_urls,
                        total_urls=get_field('total_urls', discovery_result),
                        max_depth=get_field('max_depth', discovery_result),
                        url_graph=url_graph,
                        error=get_field('error'),
                        created_at=get_field('created_at'),
                        updated_at=get_field('updated_at')
                    )
                    return task
            except json.JSONDecodeError as e:
                logger.error(f"Error decoding JSON from database: {str(e)}")
            except Exception as e:
                logger.error(f"Error creating discovery task from database: {str(e)}")
                logger.error(f"Task data: {db_task}")
                logger.error(f"Discovery result: {discovery_result}")

        return None
    
    async def update_task(
        self,
        task_id: str,
        updates: Dict
    ) -> Optional[CrawlTask]:
        """Update task attributes and persist to database"""
        task = await self.get_task(task_id)
        if not task:
            return None
        
        async with self._lock:
            for key, value in updates.items():
                if hasattr(task, key):
                    if key == "metrics" and isinstance(value, dict):
                        # Merge metrics rather than replace
                        current_metrics = getattr(task, key, {})
                        current_metrics.update(value)
                        setattr(task, key, current_metrics)
                    else:
                        setattr(task, key, value)
            task.updated_at = datetime.now()
            
            # Save to database
            await self.snowflake.save_task({
                'task_id': task.task_id,
                'task_type': 'crawl',
                'status': task.status,
                'progress': task.progress,
                'metrics': task.metrics,
                'error': task.error,
                'current_url': getattr(task, 'current_url', None),
                'urls': task.urls,
                'settings': task.settings.dict() if task.settings else {}
            })
        
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

                                    # Save content files
                                    storage_manager = StorageManager(self.snowflake)

                                    # Save markdown if available
                                    if hasattr(result, 'markdown') and result.markdown:
                                        await storage_manager.save_file(
                                            url=result.url,
                                            file_type='markdown',
                                            content=result.markdown,
                                            content_type='text/markdown',
                                            metadata={'type': 'content'}
                                        )
                                        task.metrics['saved_content']['markdown'] += 1

                                    # Save screenshot if enabled and available
                                    if task.settings.capture_screenshots and result.screenshot:
                                        await storage_manager.save_file(
                                            url=result.url,
                                            file_type='screenshot',
                                            content=result.screenshot,
                                            content_type='image/png',
                                            metadata={'type': 'screenshot'}
                                        )
                                        task.metrics['saved_content']['screenshot'] += 1

                                    # Save PDF if enabled and available
                                    if task.settings.generate_pdfs and result.pdf:
                                        await storage_manager.save_file(
                                            url=result.url,
                                            file_type='pdf',
                                            content=result.pdf,
                                            content_type='application/pdf',
                                            metadata={'type': 'document'}
                                        )
                                        task.metrics['saved_content']['pdf'] += 1

                                    # Save images if available
                                    if task.settings.save_images and result.media and 'images' in result.media:
                                        for idx, image in enumerate(result.media['images']):
                                            if image.get('data'):
                                                await storage_manager.save_file(
                                                    url=f"{result.url}#image{idx}",
                                                    file_type='image',
                                                    content=image['data'],
                                                    content_type=image.get('type', 'image/jpeg'),
                                                    metadata={
                                                        'type': 'image',
                                                        'original_src': image.get('src'),
                                                        'alt': image.get('alt')
                                                    }
                                                )
                                                task.metrics['saved_content']['images'] += 1

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
            # async with self._lock:
            #     self.active_tasks.pop(task.task_id, None)