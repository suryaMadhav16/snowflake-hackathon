import asyncio
import logging
from pathlib import Path
from typing import Dict, Set
from urllib.parse import urlparse, urljoin
import aiohttp
from bs4 import BeautifulSoup

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import NoExtractionStrategy

from utils.monitors import MemoryMonitor, AntiBot
from utils.database import DatabaseHandler
from utils.processor import ContentProcessor

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class WebScraper:
    def __init__(
        self,
        base_url: str,
        output_dir: str,
        max_concurrent: int = 5,
        requests_per_second: float = 2.0,
        memory_threshold_mb: int = 1000,
        batch_size: int = 10
    ):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        
        # Initialize utilities
        self.domain_dir = self.output_dir / self.domain
        self.domain_dir.mkdir(parents=True, exist_ok=True)
        
        self.memory_monitor = MemoryMonitor(memory_threshold_mb)
        self.anti_bot = AntiBot(requests_per_second)
        self.db = DatabaseHandler(self.domain_dir / 'stats.db')
        self.processor = ContentProcessor(self.domain_dir, self.domain)
        
        # Initialize state
        self.crawled_urls: Set[str] = set()
        self.failed_urls: Dict[str, Dict] = {}
        self.discovered_urls: Set[str] = set()
        
        # Initialize crawl ID
        self.crawl_id = None

    async def discover_sitemap_urls(self) -> Set[str]:
        """Discover URLs from sitemap.xml"""
        sitemap_url = urljoin(self.base_url, '/sitemap.xml')
        discovered_urls = set()
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(sitemap_url) as response:
                    if response.status == 200:
                        text = await response.text()
                        soup = BeautifulSoup(text, 'xml')
                        
                        # Process standard sitemap
                        for loc in soup.find_all('loc'):
                            url = loc.text.strip()
                            if urlparse(url).netloc == self.domain:
                                discovered_urls.add(url)
                        
                        # Process sitemap index if present
                        for sitemap in soup.find_all('sitemap'):
                            loc = sitemap.find('loc')
                            if loc:
                                sub_sitemap_url = loc.text.strip()
                                async with session.get(sub_sitemap_url) as sub_response:
                                    if sub_response.status == 200:
                                        sub_text = await sub_response.text()
                                        sub_soup = BeautifulSoup(sub_text, 'xml')
                                        for sub_loc in sub_soup.find_all('loc'):
                                            url = sub_loc.text.strip()
                                            if urlparse(url).netloc == self.domain:
                                                discovered_urls.add(url)
                        
                        logger.info(f"Discovered {len(discovered_urls)} URLs from sitemap")
        except Exception as e:
            logger.warning(f"Failed to fetch sitemap: {str(e)}")
        
        return discovered_urls

    async def process_url(self, url: str, browser_config: BrowserConfig) -> bool:
        """Process a single URL"""
        try:
            if url in self.crawled_urls:
                return True

            logger.info(f"Crawling: {url}")
            
            # Create new crawler for each URL with updated config
            async with AsyncWebCrawler(config=browser_config) as crawler:
                crawl_config = CrawlerRunConfig(
                    cache_mode=CacheMode.BYPASS,
                    extraction_strategy=NoExtractionStrategy(),
                    magic=True,
                    page_timeout=30000,
                    simulate_user=True
                )
                
                result = await crawler.arun(url=url, config=crawl_config)

                if not result.success:
                    self.failed_urls[url] = {
                        'error': result.error_message,
                        'timestamp': result.timestamp if hasattr(result, 'timestamp') else None
                    }
                    self.db.save_page_metadata(
                        url=url,
                        title='',
                        filepath='',
                        images=[],
                        links=[],
                        status='failed',
                        error_message=result.error_message
                    )
                    self.db.update_crawl_stats(self.crawl_id, success=False)
                    return False

                # Extract title and save markdown
                title = self.processor.extract_title(result.html, url)
                markdown_path = self.processor.save_markdown(result.markdown_v2.raw_markdown, url)

                # Process images and PDFs
                images = await self.processor.process_images(result.html, url)
                pdfs = await self.processor.process_pdfs(result.html, url)

                # Extract and process links
                internal_links, external_links = self.processor.extract_links(result.html, url)
                self.discovered_urls.update(internal_links)

                # Save metadata
                self.db.save_page_metadata(
                    url=url,
                    title=title,
                    filepath=str(markdown_path),
                    images=images + pdfs,  # Combine both types of downloads
                    links=internal_links + external_links
                )
                
                self.crawled_urls.add(url)
                self.db.update_crawl_stats(self.crawl_id, success=True)
                return True

        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            self.failed_urls[url] = {
                'error': str(e),
                'timestamp': None
            }
            self.db.save_page_metadata(
                url=url,
                title='',
                filepath='',
                images=[],
                links=[],
                status='failed',
                error_message=str(e)
            )
            self.db.update_crawl_stats(self.crawl_id, success=False)
            return False

    async def process_batch(self, urls: Set[str]):
        """Process a batch of URLs"""
        tasks = []
        for url in urls:
            if len(tasks) >= self.batch_size:
                # Wait for current batch to complete
                await asyncio.gather(*tasks)
                tasks = []
                
                # Check memory usage
                if not self.memory_monitor.check_memory():
                    logger.warning(f"High memory usage ({self.memory_monitor.get_memory_usage():.1f} MB). Pausing...")
                    await asyncio.sleep(5)
            
            if url not in self.crawled_urls:
                # Create new browser config for each request
                browser_config = BrowserConfig(
                    headless=True,
                    verbose=True,
                    extra_args=[
                        "--disable-gpu",
                        "--disable-dev-shm-usage",
                        "--no-sandbox"
                    ],
                    user_agent=self.anti_bot.get_random_user_agent()
                )
                
                tasks.append(self.process_url(url, browser_config))
                await self.anti_bot.random_delay()
        
        # Process remaining tasks
        if tasks:
            await asyncio.gather(*tasks)

    async def crawl(self):
        """Main crawling function"""
        logger.info(f"Starting crawl of {self.base_url}")
        
        # Start new crawl in database
        self.crawl_id = self.db.start_crawl()
        
        try:
            # Discover initial URLs
            self.discovered_urls = {self.base_url}
            sitemap_urls = await self.discover_sitemap_urls()
            self.discovered_urls.update(sitemap_urls)
            
            while self.discovered_urls:
                # Get batch of URLs
                batch_urls = set()
                while self.discovered_urls and len(batch_urls) < self.batch_size:
                    url = self.discovered_urls.pop()
                    if url not in self.crawled_urls:
                        batch_urls.add(url)
                
                if not batch_urls:
                    break
                
                # Process batch
                await self.process_batch(batch_urls)
                
                # Log memory usage
                logger.info(f"Memory usage: {self.memory_monitor.get_memory_usage():.1f} MB")
            
        except Exception as e:
            logger.error(f"Critical error during crawling: {str(e)}")
            
        finally:
            # End crawl in database
            self.db.end_crawl(self.crawl_id)
            
            # Log final summary
            stats = self.db.get_crawl_stats(self.crawl_id)
            logger.info(f"""
            Crawling completed:
            - Total URLs: {stats.get('total_urls', 0)}
            - Successful: {stats.get('successful', 0)}
            - Failed: {stats.get('failed', 0)}
            - Final Memory Usage: {self.memory_monitor.get_memory_usage():.1f} MB
            """)
    
    def close(self):
        """Clean up resources"""
        self.db.close()
