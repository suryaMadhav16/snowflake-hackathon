import asyncio
import logging
import base64
import re
from typing import List, Dict, AsyncGenerator, Union, Optional, Set
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
from utils.content_processor import ContentProcessor
from database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class BatchCrawler:
    """A class for handling batched web crawling operations with resource management.
    
    This class provides functionality for crawling multiple URLs in batches, with
    configurable browser settings, crawl parameters, and URL filtering. It manages
    resources efficiently and provides detailed metrics about the crawling process.

    Attributes:
        browser_config (BrowserConfig): Configuration for the browser instance.
        crawl_config (CrawlerRunConfig): Configuration for crawling behavior.
        excluded_patterns (List[re.Pattern]): List of compiled regex patterns for URL exclusion.
        db (DatabaseManager): Database manager instance for storing results.
        content_processor (ContentProcessor): Processor for handling crawled content.
        metrics (Dict): Dictionary containing crawling metrics and statistics.

    Example:
        >>> crawler = BatchCrawler(
        ...     base_url="https://example.com",
        ...     excluded_patterns=["^/admin/", "^/private/"]
        ... )
        >>> urls = ["https://example.com/page1", "https://example.com/page2"]
        >>> async for results in crawler.process_batch(urls, batch_size=2):
        ...     print(f"Processed {len(results)} URLs")
    """
    
    def __init__(
        self, 
        browser_config: BrowserConfig = None,
        crawl_config: CrawlerRunConfig = None,
        base_url: str = None,
        db: DatabaseManager = None,
        excluded_patterns: List[str] = None
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
            screenshot=True,
            wait_until="networkidle",
            pdf=True
        )
        
        # Compile excluded patterns
        self.excluded_patterns = []
        if excluded_patterns:
            for pattern in excluded_patterns:
                try:
                    self.excluded_patterns.append(re.compile(pattern))
                    logger.info(f"Added exclusion pattern: {pattern}")
                except re.error as e:
                    logger.error(f"Invalid regex pattern '{pattern}': {str(e)}")
        
        # Initialize database and content processor
        self.db = db or DatabaseManager()
        self.content_processor = None
        if base_url:
            domain = urlparse(base_url).netloc
            self.content_processor = ContentProcessor(domain, self.db)
        
        self.metrics = {
            'successful': 0,
            'failed': 0,
            'skipped': 0,
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
    
    def should_skip_url(self, url: str) -> bool:
        """Check if a URL matches any of the configured exclusion patterns.

        Args:
            url (str): The URL to check against exclusion patterns.

        Returns:
            bool: True if the URL should be skipped (matches an exclusion pattern),
                 False otherwise.

        Example:
            >>> crawler = BatchCrawler(excluded_patterns=["^/admin/"])
            >>> crawler.should_skip_url("https://example.com/admin/users")
            True
            >>> crawler.should_skip_url("https://example.com/public/page")
            False
        """
        if not self.excluded_patterns:
            return False
            
        try:
            parsed = urlparse(url)
            path = parsed.path.strip('/')
            
            for pattern in self.excluded_patterns:
                if pattern.search(path):
                    logger.info(f"Skipping URL due to exclusion pattern '{pattern.pattern}': {url}")
                    self.metrics['skipped'] += 1
                    return True
            return False
            
        except Exception as e:
            logger.error(f"Error checking URL exclusion for {url}: {str(e)}")
            return False
    
    def filter_batch(self, urls: List[str]) -> List[str]:
        """Filter a batch of URLs, removing those that match exclusion patterns.

        Args:
            urls (List[str]): List of URLs to filter.

        Returns:
            List[str]: Filtered list of URLs that don't match any exclusion patterns.

        Example:
            >>> crawler = BatchCrawler(excluded_patterns=["^/private/"])
            >>> urls = ["https://example.com/private/doc", "https://example.com/public/doc"]
            >>> crawler.filter_batch(urls)
            ['https://example.com/public/doc']
        """
        if not self.excluded_patterns:
            return urls
            
        filtered = []
        for url in urls:
            if not self.should_skip_url(url):
                filtered.append(url)
        
        if len(urls) != len(filtered):
            logger.info(f"Filtered out {len(urls) - len(filtered)} URLs based on exclusion patterns")
            
        return filtered
    
    async def process_batch(
        self, 
        urls: List[str], 
        batch_size: int = 10
    ) -> AsyncGenerator[List[CrawlResult], None]:
        """Process a list of URLs in batches, crawling and storing the results.

        This method handles the main crawling workflow, including:
        - URL filtering
        - Batch processing
        - Result storage
        - Content processing
        - Metrics tracking

        Args:
            urls (List[str]): List of URLs to process.
            batch_size (int, optional): Number of URLs to process in each batch.
                Defaults to 10.

        Yields:
            List[CrawlResult]: List of successful crawl results for each batch.

        Raises:
            Exception: If there's an error processing a batch.

        Example:
            >>> urls = ["https://example.com/page1", "https://example.com/page2"]
            >>> async for results in crawler.process_batch(urls, batch_size=2):
            ...     for result in results:
            ...         print(f"Crawled {result.url}: {len(result.content)} bytes")
        """
        # Filter URLs before processing
        urls = self.filter_batch(urls)
        total_urls = len(urls)
        total_batches = (total_urls - 1) // batch_size + 1
        
        logger.info(f"Starting batch processing of {total_urls} URLs")
        
        self.metrics['start_time'] = datetime.now()
        self.metrics['total_batches'] = total_batches
        self.metrics['current_batch'] = 0
        
        # Initialize content processor if needed
        if not self.content_processor and urls:
            domain = urlparse(urls[0]).netloc
            self.content_processor = ContentProcessor(domain, self.db)
        
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
                    
                    # Process results
                    processed_results = []
                    for result in results:
                        try:
                            if isinstance(result, CrawlResult):
                                if result.success:
                                    self.metrics['successful'] += 1
                                    await self.db.save_results([result])
                                    
                                    if self.content_processor:
                                        saved_files = await self.content_processor.save_content(result)
                                        for content_type, files in saved_files.items():
                                            if content_type in self.metrics['saved_content']:
                                                self.metrics['saved_content'][content_type] += len(files)
                                    processed_results.append(result)
                                else:
                                    self.metrics['failed'] += 1
                                    logger.warning(f"Crawl failed for {result.url}: {result.error_message}")
                            else:
                                logger.warning(f"Unexpected result type: {type(result)}")
                                self.metrics['failed'] += 1
                                
                        except Exception as e:
                            logger.error(f"Error processing result for {getattr(result, 'url', 'unknown')}: {str(e)}")
                            self.metrics['failed'] += 1
                    
                    batch_duration = (datetime.now() - batch_start).total_seconds()
                    logger.info(
                        f"Processed batch {self.metrics['current_batch']}/{total_batches} "
                        f"in {batch_duration:.2f}s"
                    )
                    
                    # Update metrics
                    self.metrics['end_time'] = datetime.now()
                    duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
                    self.metrics['duration'] = duration
                    total_processed = self.metrics['successful'] + self.metrics['failed']
                    self.metrics['urls_per_second'] = total_processed / duration if duration > 0 else 0
                    
                    yield processed_results
                    
            except Exception as e:
                logger.error(f"Error processing batch: {str(e)}", exc_info=True)
                self.metrics['failed'] += len(batch)
        
        # Final metrics update
        self.metrics['end_time'] = datetime.now()
        duration = (self.metrics['end_time'] - self.metrics['start_time']).total_seconds()
        self.metrics['duration'] = duration
        total_processed = self.metrics['successful'] + self.metrics['failed']
        self.metrics['urls_per_second'] = total_processed / duration if duration > 0 else 0
        
        # Log final stats
        logger.info(
            f"Crawling complete - Processed: {total_processed}, "
            f"Successful: {self.metrics['successful']}, "
            f"Failed: {self.metrics['failed']}, "
            f"Skipped: {self.metrics['skipped']}"
        )
        logger.info(f"Content saving summary: {self.metrics['saved_content']}")
    
    def get_metrics(self) -> Dict:
        """Get the current crawling metrics and statistics.

        Returns:
            Dict: Dictionary containing various metrics including:
                - successful: Number of successfully crawled URLs
                - failed: Number of failed crawl attempts
                - skipped: Number of skipped URLs
                - start_time: Start time of crawling
                - end_time: End time of crawling
                - current_batch: Current batch number
                - total_batches: Total number of batches
                - duration: Total crawling duration in seconds
                - urls_per_second: Average processing rate
                - saved_content: Count of saved content by type

        Example:
            >>> metrics = crawler.get_metrics()
            >>> print(f"Processed {metrics['successful']} URLs successfully")
            >>> print(f"Average rate: {metrics['urls_per_second']:.2f} URLs/second")
        """
        return self.metrics.copy()
