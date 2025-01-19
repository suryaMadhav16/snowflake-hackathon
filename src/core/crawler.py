import asyncio
import logging
import base64
from typing import List, Dict, AsyncGenerator, Union, Optional
from datetime import datetime
from urllib.parse import urlparse
from pathlib import Path
from crawl4ai import (
    AsyncWebCrawler, 
    BrowserConfig, 
    CrawlerRunConfig,
    CrawlResult,
    CacheMode
)

logger = logging.getLogger(__name__)

class ContentSaver:
    """Handles saving of crawled content"""
    
    def __init__(self, domain: str):
        self.domain = domain
        self.base_dir = Path('/tmp/webscrapper') / domain
        
        # Create directories for different content types
        self.dirs = {
            'markdown': self.base_dir / 'markdown',
            'images': self.base_dir / 'images',
            'pdfs': self.base_dir / 'pdfs',
            'screenshots': self.base_dir / 'screenshots'
        }
        
        # Ensure all directories exist
        for directory in self.dirs.values():
            directory.mkdir(parents=True, exist_ok=True)
            
        logger.info(f"Initialized content directories at {self.base_dir}")

    def _decode_base64(self, data: Union[str, bytes]) -> Optional[bytes]:
        """Safely decode base64 data"""
        if not data:
            return None
            
        try:
            if isinstance(data, str):
                # Remove data URL prefix if present
                if ',' in data:
                    data = data.split(',', 1)[1]
                # Remove any whitespace
                data = data.strip()
                return base64.b64decode(data)
            elif isinstance(data, bytes):
                return data
            else:
                logger.warning(f"Unexpected data type for base64 content: {type(data)}")
                return None
        except Exception as e:
            logger.warning(f"Failed to decode base64 data: {str(e)}")
            return None

    async def save_content(self, result: CrawlResult) -> Dict[str, Union[Path, List[Path]]]:
        """Save all content from a crawl result"""
        saved_paths = {}
        
        try:
            # Log debugging information
            logger.debug(f"Processing content for URL: {result.url}")
            logger.debug(f"Available fields: {result.dict().keys()}")
            
            # Save markdown content
            if result.markdown_v2:
                try:
                    content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else str(result.markdown_v2)
                    md_path = self.dirs['markdown'] / f"{abs(hash(result.url))}.md"
                    md_path.write_text(content, encoding='utf-8')
                    saved_paths['markdown'] = md_path.relative_to(self.base_dir)
                    logger.debug(f"Saved markdown content to {md_path}")
                except Exception as e:
                    logger.error(f"Failed to save markdown for {result.url}: {str(e)}")

            # Save images from media dictionary
            if result.media and isinstance(result.media, dict) and 'images' in result.media:
                image_paths = []
                for idx, img in enumerate(result.media['images']):
                    try:
                        if isinstance(img, dict) and 'data' in img and img.get('src'):
                            img_data = self._decode_base64(img['data'])
                            if img_data:
                                img_path = self.dirs['images'] / f"{abs(hash(str(img['src'])))}.png"
                                img_path.write_bytes(img_data)
                                image_paths.append(img_path.relative_to(self.base_dir))
                                logger.debug(f"Saved image {idx + 1} to {img_path}")
                    except Exception as e:
                        logger.warning(f"Failed to save image {idx + 1} from {result.url}: {str(e)}")
                if image_paths:
                    saved_paths['images'] = image_paths

            # Save PDF content
            if result.pdf:
                try:
                    if isinstance(result.pdf, bytes):
                        pdf_path = self.dirs['pdfs'] / f"{abs(hash(result.url))}.pdf"
                        pdf_path.write_bytes(result.pdf)
                        saved_paths['pdf'] = pdf_path.relative_to(self.base_dir)
                        logger.debug(f"Saved PDF to {pdf_path}")
                except Exception as e:
                    logger.error(f"Failed to save PDF for {result.url}: {str(e)}")

            # Save screenshot with improved error handling
            if result.screenshot is not None:
                try:
                    logger.debug(f"Screenshot type: {type(result.screenshot)}")
                    screenshot_bytes = self._decode_base64(result.screenshot)
                    
                    if screenshot_bytes:
                        ss_path = self.dirs['screenshots'] / f"{abs(hash(result.url))}.png"
                        ss_path.write_bytes(screenshot_bytes)
                        saved_paths['screenshot'] = ss_path.relative_to(self.base_dir)
                        logger.debug(f"Saved screenshot to {ss_path}")
                    else:
                        logger.warning(f"No valid screenshot data for {result.url}")
                except Exception as e:
                    logger.error(f"Failed to save screenshot for {result.url}: {str(e)}", exc_info=True)

            return saved_paths
            
        except Exception as e:
            logger.error(f"Error saving content for {result.url}: {str(e)}", exc_info=True)
            return saved_paths

class BatchCrawler:
    """Handles batched crawling operations with resource management"""
    
    def __init__(
        self, 
        browser_config: BrowserConfig = None,
        crawl_config: CrawlerRunConfig = None,
        base_url: str = None
    ):
        self.browser_config = browser_config or BrowserConfig(
            headless=True,
            browser_type="chromium",
            user_agent_mode="random",
            viewport_width=1080,
            viewport_height=800
        )
        
        self.crawl_config = crawl_config or CrawlerRunConfig(
            magic=True,
            simulate_user=True,
            cache_mode=CacheMode.ENABLED,
            mean_delay=1.0,
            max_range=0.3,
            semaphore_count=5,
            screenshot=True,  # Enable screenshots
            wait_until="networkidle"  # Wait for network to be idle
        )
        
        self.metrics = {
            'successful': 0,
            'failed': 0,
            'start_time': None,
            'end_time': None,
            'current_batch': 0,
            'total_batches': 0,
            'memory_usage': 0,
            'duration': 0,
            'urls_per_second': 0,
            'saved_content': {
                'markdown': 0,
                'images': 0,
                'pdf': 0,
                'screenshot': 0
            }
        }
        
        # Initialize content saver if base_url provided
        self.content_saver = None
        if base_url:
            domain = urlparse(base_url).netloc
            self.content_saver = ContentSaver(domain)
    
    async def process_batch(
        self, 
        urls: List[str], 
        batch_size: int = 10
    ) -> AsyncGenerator[List[CrawlResult], None]:
        """
        Process URLs in batches
        
        Args:
            urls: List of URLs to process
            batch_size: Size of each batch
            
        Yields:
            List of CrawlResult objects for each batch
        """
        total_urls = len(urls)
        total_batches = (total_urls - 1) // batch_size + 1
        
        logger.info(f"Starting batch processing of {total_urls} URLs")
        
        self.metrics['start_time'] = datetime.now()
        self.metrics['total_batches'] = total_batches
        self.metrics['current_batch'] = 0
        
        # Initialize content saver if needed
        if not self.content_saver and urls:
            domain = urlparse(urls[0]).netloc
            self.content_saver = ContentSaver(domain)
        
        # Process in batches
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            batch_start = datetime.now()
            self.metrics['current_batch'] = i // batch_size + 1
            
            try:
                async with AsyncWebCrawler(config=self.browser_config) as crawler:
                    results = await crawler.arun_many(
                        urls=batch,
                        config=self.crawl_config
                    )
                    
                    # Process and save content for successful results
                    for result in results:
                        try:
                            if isinstance(result, CrawlResult):
                                logger.debug(f"Processing result for URL: {result.url}")
                                logger.debug(f"Result success: {result.success}")
                                
                                if result.success:
                                    self.metrics['successful'] += 1
                                    if self.content_saver:
                                        saved_paths = await self.content_saver.save_content(result)
                                        # Update content saving metrics
                                        # Update metrics safely
                                        for content_type, paths in saved_paths.items():
                                            if content_type not in self.metrics['saved_content']:
                                                logger.warning(f"Unexpected content type in metrics: {content_type}")
                                                continue
                                            if isinstance(paths, list):
                                                self.metrics['saved_content'][content_type] += len(paths)
                                            else:
                                                self.metrics['saved_content'][content_type] += 1
                                else:
                                    self.metrics['failed'] += 1
                                    logger.warning(f"Crawl failed for {result.url}: {result.error_message}")
                            else:
                                logger.warning(f"Unexpected result type: {type(result)}")
                                self.metrics['failed'] += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing result for {getattr(result, 'url', 'unknown')}: {str(e)}", exc_info=True)
                            self.metrics['failed'] += 1
                    
                    batch_duration = (datetime.now() - batch_start).total_seconds()
                    logger.info(
                        f"Processed batch {self.metrics['current_batch']}/{total_batches} "
                        f"in {batch_duration:.2f}s"
                    )
                    
                    # Update timing metrics
                    self.metrics['end_time'] = datetime.now()
                    duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
                    self.metrics['duration'] = duration
                    total_processed = self.metrics['successful'] + self.metrics['failed']
                    self.metrics['urls_per_second'] = total_processed / duration if duration > 0 else 0
                    
                    yield results
                    
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}", exc_info=True)
                self.metrics['failed'] += len(batch)
        
        # Final metrics update
        self.metrics['end_time'] = datetime.now()
        duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
        self.metrics['duration'] = duration
        total_processed = self.metrics['successful'] + self.metrics['failed']
        self.metrics['urls_per_second'] = total_processed / duration if duration > 0 else 0
        
        # Log content saving summary
        logger.info(f"Content saving summary: {self.metrics['saved_content']}")
    
    def get_metrics(self) -> Dict:
        """Get current crawling metrics"""
        return self.metrics.copy()