import os
import asyncio
import json
import logging
import traceback
from pathlib import Path
from typing import List, Set, Dict, Optional
from urllib.parse import urljoin, urlparse
import aiohttp
from datetime import datetime

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.extraction_strategy import NoExtractionStrategy
from bs4 import BeautifulSoup

from .page_processor import PageProcessor
from .utils import RateLimiter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ParallelCrawler:
    def __init__(
        self,
        base_url: str,
        output_dir: str,
        groq_api_key: str,
        max_concurrent: int = 5,
        test_mode: bool = False,
        max_retries: int = 3,
        requests_per_second: float = 2.0
    ):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.output_dir = Path(output_dir)
        self.max_concurrent = max_concurrent
        self.test_mode = test_mode
        self.max_retries = max_retries
        
        # Rate limiting
        self.rate_limiter = RateLimiter(requests_per_second)
        
        # Verify GROQ API key
        if not groq_api_key:
            raise ValueError("GROQ_API_KEY is required")
        
        # Crawling state
        self.crawled_urls: Set[str] = set()
        self.failed_urls: Dict[str, Dict] = {}  # URL -> {count: int, errors: List[str]}
        self.discovered_urls: Set[str] = {base_url}
        
        # Initialize processor
        self.page_processor = PageProcessor(groq_api_key)
        
        # Setup directories
        self._setup_directories()
        
        # Load previous state if exists
        self._load_state()

    def _setup_directories(self):
        """Create necessary directory structure"""
        logger.info(f"Setting up directories in {self.output_dir}")
        self.raw_dir = self.output_dir / "raw" / self.domain
        self.processed_dir = self.output_dir / "processed" / self.domain
        self.images_dir = self.output_dir / "images" / self.domain
        self.state_file = self.output_dir / f"{self.domain}_crawl_state.json"
        
        for directory in [self.raw_dir, self.processed_dir, self.images_dir]:
            directory.mkdir(parents=True, exist_ok=True)
            logger.debug(f"Created directory: {directory}")

    def _load_state(self):
        """Load previous crawl state if exists"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                    self.crawled_urls = set(state.get('crawled_urls', []))
                    self.failed_urls = state.get('failed_urls', {})
                    self.discovered_urls = set(state.get('discovered_urls', [self.base_url]))
                logger.info(f"Loaded previous state: {len(self.crawled_urls)} crawled, {len(self.failed_urls)} failed")
                logger.debug(f"Failed URLs: {json.dumps(self.failed_urls, indent=2)}")
            except Exception as e:
                logger.error(f"Error loading state: {str(e)}")
                logger.error(traceback.format_exc())

    def _save_state(self):
        """Save current crawl state"""
        try:
            state = {
                'crawled_urls': list(self.crawled_urls),
                'failed_urls': self.failed_urls,
                'discovered_urls': list(self.discovered_urls),
                'last_updated': datetime.utcnow().isoformat()
            }
            with open(self.state_file, 'w') as f:
                json.dump(state, f, indent=2)
            logger.debug("Saved crawler state")
        except Exception as e:
            logger.error(f"Error saving state: {str(e)}")
            logger.error(traceback.format_exc())

    def _get_page_dirs(self, url: str) -> tuple[Path, Path, Path]:
        """Get directories for a specific page"""
        url_hash = str(abs(hash(url)) % 10000)
        page_raw_dir = self.raw_dir / url_hash
        page_processed_dir = self.processed_dir / url_hash
        page_images_dir = self.images_dir / url_hash
        
        for directory in [page_raw_dir, page_processed_dir, page_images_dir]:
            directory.mkdir(exist_ok=True)
            
        return page_raw_dir, page_processed_dir, page_images_dir

    async def _process_page(self, url: str, result, parent_url: Optional[str] = None):
        """Process a single page result"""
        try:
            if not result.success:
                logger.error(f"Failed to crawl {url}: {result.error_message}")
                if url not in self.failed_urls:
                    self.failed_urls[url] = {'count': 0, 'errors': []}
                self.failed_urls[url]['count'] += 1
                self.failed_urls[url]['errors'].append(result.error_message)
                return False

            # Get page directories
            raw_dir, processed_dir, images_dir = self._get_page_dirs(url)
            
            # Save raw markdown
            if result.markdown_v2:
                markdown_path = raw_dir / "content.md"
                with open(markdown_path, 'w', encoding='utf-8') as f:
                    f.write(result.markdown_v2.raw_markdown)
                logger.debug(f"Saved markdown content: {markdown_path}")

                # Extract and process URLs
                soup = BeautifulSoup(result.markdown_v2.raw_markdown, 'html.parser')
                for a in soup.find_all('a', href=True):
                    new_url = urljoin(url, a['href'])
                    if urlparse(new_url).netloc == self.domain:
                        self.discovered_urls.add(new_url)
                        logger.debug(f"Discovered new URL: {new_url}")

                # Process page with Groq
                await self.page_processor.process_page(
                    url=url,
                    markdown_content=result.markdown_v2.raw_markdown,
                    markdown_path=str(markdown_path.relative_to(self.raw_dir)),
                    images_dir=str(images_dir.relative_to(self.images_dir)),
                    parent_url=parent_url
                )

                self.crawled_urls.add(url)
                logger.info(f"Successfully processed page: {url}")
                return True
            else:
                logger.warning(f"No markdown content for {url}")
                return False

        except Exception as e:
            logger.error(f"Error processing page {url}: {str(e)}")
            logger.error(traceback.format_exc())
            if url not in self.failed_urls:
                self.failed_urls[url] = {'count': 0, 'errors': []}
            self.failed_urls[url]['count'] += 1
            self.failed_urls[url]['errors'].append(str(e))
            return False

    async def crawl(self):
        """Main crawling function"""
        logger.info(f"Starting crawl of {self.base_url}")
        
        browser_config = BrowserConfig(
            headless=True,
            verbose=True,
            extra_args=[
                "--disable-gpu",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-setuid-sandbox",
            ],
        )
        
        crawl_config = CrawlerRunConfig(
            cache_mode=CacheMode.BYPASS,
            extraction_strategy=NoExtractionStrategy()
        )

        try:
            async with AsyncWebCrawler(config=browser_config) as crawler:
                logger.info("Initialized web crawler")
                
                semaphore = asyncio.Semaphore(self.max_concurrent)
                
                async def process_url(url: str):
                    async with semaphore:
                        await self.rate_limiter.wait()
                        
                        try:
                            if url in self.crawled_urls:
                                logger.debug(f"Already crawled: {url}")
                                return
                                
                            logger.info(f"Crawling: {url}")
                            result = await crawler.arun(url=url, config=crawl_config)
                            await self._process_page(url, result)
                            
                        except Exception as e:
                            logger.error(f"Error processing {url}: {str(e)}")
                            logger.error(traceback.format_exc())
                            if url not in self.failed_urls:
                                self.failed_urls[url] = {'count': 0, 'errors': []}
                            self.failed_urls[url]['count'] += 1
                            self.failed_urls[url]['errors'].append(str(e))
                        
                        finally:
                            self._save_state()

                # Main crawling loop
                while self.discovered_urls:
                    if self.test_mode and len(self.crawled_urls) >= 5:
                        logger.info("Test mode: reached 5 pages limit")
                        break
                    
                    # Get URLs to process
                    urls_to_process = set()
                    while self.discovered_urls and len(urls_to_process) < self.max_concurrent:
                        url = self.discovered_urls.pop()
                        if (url not in self.crawled_urls and 
                            (url not in self.failed_urls or 
                             self.failed_urls[url]['count'] < self.max_retries)):
                            urls_to_process.add(url)
                            logger.debug(f"Added to processing queue: {url}")
                    
                    if not urls_to_process:
                        logger.info("No more URLs to process")
                        break
                    
                    # Process URLs in parallel
                    tasks = [process_url(url) for url in urls_to_process]
                    await asyncio.gather(*tasks)
            
        except Exception as e:
            logger.error(f"Critical error during crawling: {str(e)}")
            logger.error(traceback.format_exc())
            
        finally:
            # Final state save
            self._save_state()
            
            # Log summary with details
            logger.info(f"""
            Crawling completed:
            - Pages crawled: {len(self.crawled_urls)}
            - Pages failed: {len(self.failed_urls)}
            - Failed URLs and errors:
            {json.dumps(self.failed_urls, indent=2)}
            """)
