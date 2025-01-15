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
        batch_size: int = 10,
        test_mode: bool = False
    ):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        self.test_mode = test_mode
        
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
        
        # Initialize crawl ID and override URLs
        self.crawl_id = None
        self.override_discovered_urls = None

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

    async def crawl(self):
        """Main crawling function"""
        logger.info(f"Starting crawl of {self.base_url}")
        
        # Start new crawl in database
        self.crawl_id = self.db.start_crawl()
        
        try:
            # Initialize discovered URLs
            if self.override_discovered_urls:
                self.discovered_urls = set(self.override_discovered_urls)
                logger.info(f"Using {len(self.discovered_urls)} pre-filtered URLs")
            else:
                self.discovered_urls = {self.base_url}
                sitemap_urls = await self.discover_sitemap_urls()
                self.discovered_urls.update(sitemap_urls)
            
            while self.discovered_urls:
                # Check test mode limit
                if self.test_mode and len(self.crawled_urls) >= 15:
                    logger.info("Test mode: reached 15 pages limit")
                    break
                
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
                
                # Update memory usage in database
                current_memory = self.memory_monitor.get_memory_usage()
                self.db.update_memory_usage(self.crawl_id, current_memory)
                logger.info(f"Memory usage: {current_memory:.1f} MB")
        
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