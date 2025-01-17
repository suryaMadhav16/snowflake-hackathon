import asyncio
import logging
import re
from pathlib import Path
from typing import Dict, Set, List
from urllib.parse import urlparse, urljoin

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

from utils.monitors import MemoryMonitor, AntiBot
from utils.database import DatabaseHandler
from utils.processor import ContentProcessor

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(
        self,
        base_url: str,
        output_dir: str,
        exclusion_patterns: List[str] = None,
        max_concurrent: int = 100,
        requests_per_second: float = 10.0,
        memory_threshold_mb: int = 4000,
        batch_size: int = 200,
        test_mode: bool = False,
        browser_type: str = "chromium",
        enable_screenshots: bool = True,
        enable_pdfs: bool = True,
        enable_magic: bool = True,
        simulate_user: bool = True
    ):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        self.test_mode = test_mode
        
        # Reset and initialize database
        self.db_path = Path(output_dir) / self.domain / 'stats.db'
        if self.db_path.exists():
            self.db_path.unlink()
            
        # Initialize utilities
        self.domain_dir = self.output_dir / self.domain
        self.domain_dir.mkdir(parents=True, exist_ok=True)
        
        self.memory_monitor = MemoryMonitor(memory_threshold_mb)
        self.anti_bot = AntiBot(requests_per_second)
        self.db = DatabaseHandler(self.domain_dir / 'stats.db')
        self.processor = ContentProcessor(self.domain_dir, self.domain)
        
        # Store configuration
        self.config = {
            'max_concurrent': max_concurrent,
            'requests_per_second': requests_per_second,
            'memory_threshold_mb': memory_threshold_mb,
            'batch_size': batch_size,
            'test_mode': test_mode,
            'browser_type': browser_type,
            'enable_screenshots': enable_screenshots,
            'enable_pdfs': enable_pdfs,
            'enable_magic': enable_magic,
            'simulate_user': simulate_user
        }
        
        # Compile exclusion patterns
        self.exclusion_patterns = []
        if exclusion_patterns:
            for pattern in exclusion_patterns:
                pattern = pattern.strip()
                if pattern:
                    try:
                        self.exclusion_patterns.append(re.compile(pattern))
                    except re.error:
                        logger.warning(f"Invalid regex pattern: {pattern}")
        
        # Setup markdown generator
        self.markdown_generator = DefaultMarkdownGenerator(
            options={
                "ignore_links": False,
                "escape_html": True,
                "body_width": 80,
                "skip_internal_links": False
            }
        )
        
        # Enhanced browser configuration
        self.browser_config = BrowserConfig(
            headless=True,
            browser_type=browser_type,
            viewport_width=1080,
            viewport_height=800,
            verbose=False,
            extra_args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-extensions",
                "--disable-background-networking",
                "--disable-default-apps",
                "--disable-sync",
                "--disable-translate",
                "--hide-scrollbars",
                "--metrics-recording-only",
                "--mute-audio",
                "--no-first-run",
                "--safebrowsing-disable-auto-update"
            ]
        )
        
        # Enhanced crawler configuration
        self.run_config = CrawlerRunConfig(
            word_count_threshold=10,
            cache_mode="ENABLED",
            screenshot=enable_screenshots,
            pdf=enable_pdfs,
            magic=enable_magic,
            simulate_user=simulate_user,
            page_timeout=30000,
            markdown_generator=self.markdown_generator
        )
        
        # Initialize state
        self.crawled_urls: Set[str] = set()
        self.failed_urls: Dict[str, Dict] = {}
        self.discovered_urls: Set[str] = set()
        self.crawl_id = None
        self.current_batch_size = self.batch_size
        self.performance_metrics = {
            'memory_usage': [],
            'processing_times': [],
            'success_rate': 0.0
        }

    def should_exclude_url(self, url: str) -> bool:
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.base_url, url)
        
        parsed = urlparse(url)
        if parsed.netloc != self.domain:
            return True
            
        path = parsed.path
        for pattern in self.exclusion_patterns:
            if pattern.search(path):
                logger.debug(f"Excluded URL {url} - matched pattern {pattern.pattern}")
                return True
        return False

    async def discover_sitemap_urls(self) -> Set[str]:
        sitemap_url = urljoin(self.base_url, '/sitemap.xml')
        discovered_urls = set()
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                result = await crawler.arun(sitemap_url, config=self.run_config)
                if result.success:
                    for link in result.links.get('internal', []):
                        if urlparse(link).netloc == self.domain:
                            discovered_urls.add(link)
                    
                    logger.info(f"Discovered {len(discovered_urls)} URLs from sitemap")
        except Exception as e:
            logger.warning(f"Failed to fetch sitemap: {str(e)}")
        
        return discovered_urls

    async def process_batch(self, urls: Set[str]):
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                url_list = list(urls)
                
                for i in range(0, len(url_list), self.max_concurrent):
                    current_memory = self.memory_monitor.get_memory_usage()
                    memory_ratio = current_memory / self.memory_monitor.threshold_mb
                    
                    adjusted_concurrent = int(self.max_concurrent * (1 - memory_ratio * 0.5))
                    adjusted_concurrent = max(10, min(adjusted_concurrent, self.max_concurrent))
                    
                    batch = url_list[i:i + adjusted_concurrent]
                    logger.info(f"Processing batch of {len(batch)} URLs (Memory: {current_memory:.1f}MB)")
                    
                    tasks = []
                    for url in batch:
                        session_id = f"session_{hash(str(url))}"
                        task = self.process_url(crawler, url, session_id)
                        tasks.append(task)
                    
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks),
                            timeout=len(batch) * 30
                        )
                    except asyncio.TimeoutError:
                        logger.warning("Batch processing timeout - adjusting batch size")
                        self.current_batch_size = max(10, self.current_batch_size // 2)
                    except Exception as e:
                        logger.error(f"Error in batch: {str(e)}")
                    
                    if not self.memory_monitor.check_memory():
                        pause_time = min(30, int(memory_ratio * 60))
                        logger.warning(f"High memory usage - pausing for {pause_time}s")
                        await asyncio.sleep(pause_time)
                    
                    self.performance_metrics['memory_usage'].append(current_memory)
                    await self.anti_bot.random_delay()
                    
        except Exception as e:
            logger.error(f"Error in batch processing: {str(e)}")

    async def process_url(self, crawler: AsyncWebCrawler, url: str, session_id: str):
        try:
            if not self.memory_monitor.check_memory():
                logger.warning("Memory threshold reached, skipping URL")
                return

            start_time = asyncio.get_event_loop().time()
            result = await crawler.arun(url, config=self.run_config, session_id=session_id)
            processing_time = asyncio.get_event_loop().time() - start_time

            self.performance_metrics['processing_times'].append(processing_time)

            if result.success:
                if hasattr(result, 'markdown_v2'):
                    content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else str(
                        result.markdown_v2)
                    title = str(result.title) if hasattr(result, 'title') else str(url)
                else:
                    content = result.cleaned_html if hasattr(result, 'cleaned_html') else ''
                    title = url

                filepath = self.processor.save_markdown(content, url)

                images = []
                if hasattr(result, 'media') and 'images' in result.media:
                    images = await self.processor.save_images(result.media['images'], url)

                if hasattr(result, 'pdf') and result.pdf:
                    await self.processor.save_pdf(result.pdf, url)

                # Extract URLs from internal links, handling both string and dictionary formats
                internal_links = []
                if hasattr(result, 'links'):
                    for link in result.links.get('internal', []):
                        # Handle both string URLs and dictionary link objects
                        if isinstance(link, dict):
                            link_url = link.get('url', '')  # Assuming 'url' is the key for URL in link objects
                            if link_url:
                                internal_links.append(link_url)
                        elif isinstance(link, str):
                            internal_links.append(link)

                new_urls = {
                    link for link in internal_links
                    if link not in self.crawled_urls
                       and link not in self.failed_urls
                       and not self.should_exclude_url(link)
                }

                if new_urls:
                    logger.info(f"Found {len(new_urls)} new URLs on {url}")
                    self.discovered_urls.update(new_urls)

                self.db.save_page_metadata(
                    url=url,
                    title=title,
                    filepath=str(filepath),
                    images=images,
                    links=internal_links,
                    status='success',
                    metadata=result.metrics if hasattr(result, 'metrics') else None,
                    screenshot_path=result.screenshot if hasattr(result, 'screenshot') else None,
                    pdf_path=filepath if hasattr(result, 'pdf') and result.pdf else None,
                    word_count=len(content.split()) if isinstance(content, str) else 0
                )

                self.db.update_crawl_stats(self.crawl_id, success=True)

            else:
                error_message = result.error_message if hasattr(result, 'error_message') else 'Unknown error'
                self.failed_urls[url] = {'error': error_message}
                self.db.save_page_metadata(
                    url=url,
                    title=url,
                    filepath='',
                    images=[],
                    links=[],
                    status='failed',
                    error_message=error_message
                )
                self.db.update_crawl_stats(self.crawl_id, success=False)

            self.crawled_urls.add(url)

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            self.failed_urls[url] = {'error': str(e)}
            self.db.save_page_metadata(
                url=url,
                title=url,
                filepath='',
                images=[],
                links=[],
                status='failed',
                error_message=str(e)
            )
            self.db.update_crawl_stats(self.crawl_id, success=False)
            self.crawled_urls.add(url)

    async def crawl(self):
        logger.info(f"Starting crawl of {self.base_url}")
        
        # Start new crawl in database with config
        self.crawl_id = self.db.start_crawl(config=self.config)
        
        try:
            self.discovered_urls = {self.base_url}
            sitemap_urls = await self.discover_sitemap_urls()
            self.discovered_urls.update(sitemap_urls)
            
            logger.info(f"Starting with {len(self.discovered_urls)} URLs")
            
            while self.discovered_urls:
                if self.test_mode and len(self.crawled_urls) >= 15:
                    logger.info("Test mode: reached 15 pages limit")
                    break
                
                batch_urls = set()
                while self.discovered_urls and len(batch_urls) < self.current_batch_size:
                    url = self.discovered_urls.pop()
                    if url not in self.crawled_urls:
                        batch_urls.add(url)
                
                if not batch_urls:
                    break
                
                await self.process_batch(batch_urls)
                
                current_memory = self.memory_monitor.get_memory_usage()
                self.db.update_memory_usage(self.crawl_id, current_memory)
                logger.info(f"Memory usage: {current_memory:.1f} MB")
                
                # Update performance metrics
                if len(self.crawled_urls) > 0:
                    self.performance_metrics['success_rate'] = (
                        len([url for url in self.crawled_urls if url not in self.failed_urls]) / 
                        len(self.crawled_urls) * 100
                    )
        
        except Exception as e:
            logger.error(f"Critical error during crawling: {str(e)}")
            
        finally:
            # End crawl in database with performance metrics
            self.db.end_crawl(self.crawl_id, metrics=self.performance_metrics)
            
            stats = self.db.get_crawl_stats(self.crawl_id)
            logger.info(f"""
            Crawling completed:
            - Total URLs: {stats.get('total_urls', 0)}
            - Successful: {stats.get('successful', 0)}
            - Failed: {stats.get('failed', 0)}
            - Final Memory Usage: {self.memory_monitor.get_memory_usage():.1f} MB
            """)

    def close(self):
        self.db.close()