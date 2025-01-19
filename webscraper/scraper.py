import asyncio
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Dict, Set, List, Tuple
from urllib.parse import urlparse, urljoin

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

from utils.monitors import MemoryMonitor, AntiBot
from utils.database import DatabaseHandler
from utils.processor import ContentProcessor

logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class CrawlerMode:
    """Performance modes for crawler"""
    
    LOW = {
        'max_concurrent': 5,
        'requests_per_second': 2.0,
        'memory_threshold_mb': 1000,
        'batch_size': 50
    }
    
    MEDIUM = {
        'max_concurrent': 10,
        'requests_per_second': 5.0,
        'memory_threshold_mb': 2000,
        'batch_size': 100
    }
    
    HIGH = {
        'max_concurrent': 20,
        'requests_per_second': 10.0,
        'memory_threshold_mb': 4000,
        'batch_size': 200
    }
    
    EXTREME = {
        'max_concurrent': 50,
        'requests_per_second': 20.0,
        'memory_threshold_mb': 8000,
        'batch_size': 500
    }
    
    @staticmethod
    def get_mode(mode: str) -> dict:
        """Get configuration for specified mode"""
        modes = {
            'low': CrawlerMode.LOW,
            'medium': CrawlerMode.MEDIUM,
            'high': CrawlerMode.HIGH,
            'extreme': CrawlerMode.EXTREME
        }
        return modes.get(mode.lower(), CrawlerMode.MEDIUM)
    
    
class WebScraper:
    """Enhanced web scraper using Crawl4AI"""

    async def crawl(self):
        """Main crawling method"""
        try:
            # Initialize crawl statistics
            self.crawl_id = self.db.start_crawl(self.config)
            logger.info(f"Started crawl {self.crawl_id} with config: {self.config}")
            
            # Discover initial URLs from sitemap
            self.discovered_urls = await self.discover_sitemap_urls()
            
            if self.test_mode:
                # Limit URLs in test mode
                self.discovered_urls = set(list(self.discovered_urls)[:15])
            
            total_discovered = len(self.discovered_urls)
            logger.info(f"Initial URL discovery complete. Found {total_discovered} URLs")
            
            # Process URLs in batches
            while self.discovered_urls or self.pending_urls:
                # Check memory usage and adjust batch size if needed
                if not self.memory_monitor.check_memory():
                    self.current_batch_size = max(5, self.current_batch_size // 2)
                    logger.warning(f"High memory usage - reduced batch size to {self.current_batch_size}")
                    await asyncio.sleep(30)  # Allow system to recover
                
                # Select next batch of URLs
                batch = set()
                while len(batch) < self.current_batch_size and (self.discovered_urls or self.pending_urls):
                    if self.discovered_urls:
                        url = self.discovered_urls.pop()
                    else:
                        url = self.pending_urls.pop()
                        
                    if url not in self.crawled_urls and url not in self.failed_urls:
                        batch.add(url)
                
                if not batch:
                    break
                
                # Process batch
                logger.info(f"Processing batch of {len(batch)} URLs")
                await self.process_batch(batch)
                
                # Update progress
                stats = self.db.get_crawl_stats(self.crawl_id)
                logger.info(f"Progress: {stats['successful']} successful, {stats['failed']} failed")
            
            # Finalize crawl
            logger.info("Crawl complete")
            self.db.end_crawl(self.crawl_id, self.performance_metrics)
            
        except Exception as e:
            logger.error(f"Error during crawl: {str(e)}")
            if self.crawl_id:
                self.db.end_crawl(self.crawl_id, {'error': str(e)})
            raise

    def close(self):
        """Clean up resources"""
        try:
            # Close database connection
            if hasattr(self, 'db'):
                self.db.close()
                
            # Clear sets to free memory
            self.crawled_urls.clear()
            self.failed_urls.clear()
            self.discovered_urls.clear()
            self.pending_urls.clear()
            
            logger.info("WebScraper resources cleaned up")
            
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}")

    
    def __init__(
        self,
        base_url: str,
        output_dir: str,
        exclusion_patterns: List[str] = None,
        max_concurrent: int = 10,
        requests_per_second: float = 5.0,
        memory_threshold_mb: int = 2000,
        batch_size: int = 100,
        test_mode: bool = False,
        browser_type: str = "chromium",
        enable_screenshots: bool = False,
        enable_pdfs: bool = False,
        enable_magic: bool = True,
        simulate_user: bool = True
    ):
        # URL and domain setup
        self.base_url = base_url
        self.base_domain = urlparse(base_url).netloc
        
        # Resource paths
        self.output_dir = Path(output_dir)
        self.domain_dir = self.output_dir / self.base_domain
        self.db_path = self.domain_dir / 'stats.db'
        
        # Clean up and create directories
        if self.db_path.exists():
            self.db_path.unlink()
        self.domain_dir.mkdir(parents=True, exist_ok=True)
        
        # Performance settings
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        self.test_mode = test_mode
        self.current_batch_size = batch_size
        
        # URL tracking
        self.crawled_urls = set()
        self.failed_urls = {}
        self.discovered_urls = set()
        self.pending_urls = set()
        self.crawl_id = None
        
        # Initialize utilities
        self.memory_monitor = MemoryMonitor(memory_threshold_mb)
        self.anti_bot = AntiBot(requests_per_second)
        self.db = DatabaseHandler(self.db_path)
        self.processor = ContentProcessor(self.domain_dir, self.base_domain)
        
        # Compile exclusion patterns
        self.exclusion_patterns = []
        if exclusion_patterns:
            for pattern in exclusion_patterns:
                pattern = pattern.strip()
                if pattern:
                    try:
                        self.exclusion_patterns.append(re.compile(pattern, re.IGNORECASE))
                    except re.error:
                        logger.warning(f"Invalid regex pattern: {pattern}")
        
        # Default exclusion patterns
        default_patterns = [
            r'\.(?:jpg|jpeg|png|gif|css|js|xml|txt|pdf)$',   # Common static files
            r'\?(?:.*&)?(?:utm_|ref=|sid=)',                # Tracking parameters
            r'/(?:tag|category|author|search|page)/',        # Common filter/list pages
            r'/feed/?$',                                    # RSS/Atom feeds
            r'/wp-(?:content|includes|admin)/',             # WordPress system
            r'/(?:login|logout|register|signin)/?$'         # Auth pages
        ]
        for pattern in default_patterns:
            try:
                self.exclusion_patterns.append(re.compile(pattern, re.IGNORECASE))
            except re.error as e:
                logger.warning(f"Invalid default pattern {pattern}: {e}")
                
        # Browser configuration
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
                "--metrics-recording-only",
                "--mute-audio",
                "--no-first-run",
                "--hide-scrollbars",
                "--safebrowsing-disable-auto-update"
            ]
        )
        
        # Default markdown generator
        markdown_options = {
            'ignore_links': False,
            'escape_html': True,
            'body_width': 80,
            'skip_internal_links': False
        }
        self.markdown_generator = DefaultMarkdownGenerator(options=markdown_options)
        
        # Crawler configuration
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
        
        # Performance tracking
        self.performance_metrics = {
            'memory_usage': [],
            'processing_times': [],
            'success_rate': 0.0
        }
        
        # Default config
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
            'simulate_user': simulate_user,
            'include_fragments': False  # Don't crawl URL fragments by default
        }
        
    
    def normalize_url(self, url) -> str:
        """Normalize URL format consistently across the application"""
        if isinstance(url, dict):
            url = url.get('href', '')
        url = str(url).strip()
        if not url:
            return ''
        
        try:
            # Handle relative URLs
            if not url.startswith(('http://', 'https://')):
                url = urljoin(self.base_url, url)
            
            # Parse and rebuild URL to normalize format
            parsed = urlparse(url)
            normalized = parsed._replace(
                netloc=parsed.netloc.lower(),
                path=parsed.path.replace('//', '/')
            )
            return normalized.geturl()
        except Exception as e:
            logger.error(f"Error normalizing URL {url}: {str(e)}")
            return ''

    def _clean_url(self, url: str) -> str:
        """Clean and normalize URL by removing tracking parameters and fragments"""
        # Remove common tracking parameters
        if '?' in url:
            base_url, params = url.split('?', 1)
            param_list = [p for p in params.split('&')
                         if not any(t in p.lower() for t in ['utm_', 'ref=', 'fbclid='])]
            url = base_url + ('?' + '&'.join(param_list) if param_list else '')
        
        # Remove trailing slashes and fragments
        url = url.split('#')[0].rstrip('/')
        return url

    def _is_same_domain(self, url: str) -> bool:
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            return parsed.netloc.lower() == self.base_domain.lower()
        except Exception:
            return False

    def get_links_from_result(self, result) -> Tuple[Set[str], Set[str]]:
        """Extract internal and external links from crawl result"""
        internal_links = set()
        external_links = set()
        
        if not hasattr(result, 'links'):
            return internal_links, external_links
        
        # Process all types of links
        for link_type, links in result.links.items():
            for link in links:
                # Extract URL from link object or string
                raw_url = link.get('href', '') if isinstance(link, dict) else str(link)
                url = self.normalize_url(raw_url)
                
                if not url:
                    continue
                
                if self._is_same_domain(url):
                    clean_url = self._clean_url(url)
                    if clean_url and not self.should_exclude_url(clean_url):
                        internal_links.add(clean_url)
                else:
                    external_links.add(url)
        
        return internal_links, external_links

    def should_exclude_url(self, url: str) -> bool:
        """Check if a URL should be excluded from crawling"""
        try:
            if not url:
                return True
            
            # Normalize URL first
            normalized_url = self.normalize_url(url)
            if not normalized_url:
                return True
            
            # Parse URL
            parsed = urlparse(normalized_url)
            
            # Check domain
            if parsed.netloc.lower() != self.base_domain.lower():
                logger.debug(f"Excluded external URL: {normalized_url}")
                return True
            
            # Normalize path
            path = parsed.path.strip('/').lower()
            if not path:
                path = '/'
            
            # Check path exclusions
            if any(pattern.search(path) for pattern in self.exclusion_patterns):
                logger.debug(f"Excluded URL {normalized_url} - matched exclusion pattern")
                return True
            
            # Check query parameters and fragments
            if parsed.fragment and not self.config.get('include_fragments', False):
                logger.debug(f"Excluded URL with fragment: {normalized_url}")
                return True
            
            # Check common patterns to exclude
            if re.search(r'\.(jpg|jpeg|png|gif|css|js|xml|txt|pdf)$', path, re.I):
                logger.debug(f"Excluded resource URL: {normalized_url}")
                return True
            
            # Check for admin/login/system paths
            if re.search(r'^/(admin|login|wp-|api|static|assets)/', path, re.I):
                logger.debug(f"Excluded system URL: {normalized_url}")
                return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking URL exclusion for {url}: {str(e)}")
            return True
    def process_sitemap_content(self, content: str) -> bool:
        """Check if content is a sitemap by looking for common XML indicators"""
        xml_indicators = [
            '<?xml',
            '<urlset',
            '<sitemapindex',
            'xmlns="http://www.sitemaps.org/schemas/sitemap/',
            'xmlns="http://www.google.com/schemas/sitemap/',
        ]
        content_lower = content.lower()
        return any(indicator.lower() in content_lower for indicator in xml_indicators)

    async def discover_sitemap_urls(self) -> Set[str]:
        """Discover URLs from sitemap"""
        discovered = set()
        sitemap_paths = ['/sitemap.xml', '/sitemap_index.xml', '/sitemap/sitemap.xml']
        
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                # Try each potential sitemap path
                for sitemap_path in sitemap_paths:
                    sitemap_url = urljoin(self.base_url, sitemap_path)
                    logger.info(f"Trying sitemap at: {sitemap_url}")
                    
                    try:
                        result = await crawler.arun(sitemap_url, config=self.run_config)
                        if not result.success:
                            continue
                        
                        # Check if content is a sitemap
                        is_sitemap = False
                        if hasattr(result, 'cleaned_html'):
                            is_sitemap = self.process_sitemap_content(result.cleaned_html)
                    
                        if is_sitemap:
                            logger.info(f"Found valid sitemap at {sitemap_url}")
                            sitemap_urls = set()
                        
                            # Extract URLs from sitemap
                            internal, _ = self.get_links_from_result(result)
                            for url in internal:
                                if url.endswith('.xml'):  # Handle sitemap index
                                    sub_result = await crawler.arun(url, config=self.run_config)
                                    if sub_result.success:
                                        sub_internal, _ = self.get_links_from_result(sub_result)
                                        sitemap_urls.update(sub_internal)
                                else:
                                    sitemap_urls.add(url)
                        
                            if sitemap_urls:
                                logger.info(f"Found {len(sitemap_urls)} URLs in sitemap")
                                discovered.update(sitemap_urls)
                                return discovered  # Found valid sitemap, no need to continue
                            
                    except Exception as e:
                        logger.debug(f"Error processing sitemap {sitemap_url}: {e}")
                        continue
            
                # If no sitemap found, start from homepage
                logger.info("No valid sitemap found, starting from homepage")
                result = await crawler.arun(self.base_url, config=self.run_config)
                if result.success:
                    internal, _ = self.get_links_from_result(result)
                    discovered.update(internal)
                
        except Exception as e:
            logger.error(f"Error during sitemap discovery: {e}")
        
        logger.info(f"Total discovered URLs: {len(discovered)}")
        return discovered

    async def process_batch(self, urls: Set[str]):
        """Process a batch of URLs concurrently"""
        try:
            async with AsyncWebCrawler(config=self.browser_config) as crawler:
                url_list = list(urls)
                total_urls = len(url_list)
                processed = 0
                failed_urls = set()
                
                while processed < total_urls:
                    # Check memory and adjust batch size
                    current_memory = self.memory_monitor.get_memory_usage()
                    memory_ratio = current_memory / self.memory_monitor.threshold_mb
                    
                    # Dynamic concurrency adjustment
                    adjusted_concurrent = int(self.max_concurrent * (1 - memory_ratio * 0.5))
                    adjusted_concurrent = max(5, min(adjusted_concurrent, self.max_concurrent))
                    
                    # Select next batch
                    batch_end = min(processed + adjusted_concurrent, total_urls)
                    batch = url_list[processed:batch_end]
                    
                    logger.info(
                        f"Processing batch {processed}-{batch_end} of {total_urls} URLs "
                        f"(Memory: {current_memory:.1f}MB, Concurrency: {adjusted_concurrent})"
                    )
                    
                    tasks = []
                    retries = {}
                    
                    for url in batch:
                        # Check if URL was already processed
                        cursor = self.db.conn.cursor()
                        cursor.execute('SELECT processed, status FROM pages WHERE url = ?', (url,))
                        result = cursor.fetchone()
                        
                        if not result or (result[0] == 0 and result[1] != 'success'):
                            session_id = f"session_{hash(str(url))}"
                            task = asyncio.create_task(
                                self.process_url(crawler, url, session_id)
                            )
                            tasks.append(task)
                            retries[url] = retries.get(url, 0) + 1
                    
                    if tasks:
                        try:
                            # Process batch with timeout
                            results = await asyncio.wait_for(
                                asyncio.gather(*tasks, return_exceptions=True),
                                timeout=len(tasks) * 30  # 30 seconds per URL
                            )
                            
                            # Handle results and update metrics
                            for i, result in enumerate(results):
                                url = batch[i]
                                if isinstance(result, Exception):
                                    logger.error(f"Error processing {url}: {str(result)}")
                                    if retries[url] < 3:  # Allow up to 3 retries
                                        failed_urls.add(url)
                                    else:
                                        self.failed_urls[url] = {'error': str(result)}
                                        
                            # Update performance metrics
                            self.performance_metrics['memory_usage'].append(current_memory)
                            self.performance_metrics['success_rate'] = (
                                (len(self.crawled_urls) - len(self.failed_urls)) /
                                max(len(self.crawled_urls), 1) * 100
                            )
                            
                        except asyncio.TimeoutError:
                            logger.warning("Batch processing timeout - reducing batch size")
                            self.current_batch_size = max(5, self.current_batch_size // 2)
                            failed_urls.update(batch)  # Retry these URLs later
                            
                        except Exception as e:
                            logger.error(f"Batch error: {str(e)}")
                            failed_urls.update(batch)
                            
                        finally:
                            # Memory management
                            if not self.memory_monitor.check_memory():
                                pause_time = min(30, int(memory_ratio * 60))
                                logger.warning(f"High memory usage - pausing for {pause_time}s")
                                await asyncio.sleep(pause_time)
                            
                            await self.anti_bot.random_delay()
                    
                    processed += len(batch)
                    
                    # Handle failed URLs
                    if failed_urls and processed >= total_urls:
                        remaining_retries = [url for url in failed_urls if retries.get(url, 0) < 3]
                        if remaining_retries:
                            url_list.extend(remaining_retries)
                            total_urls = len(url_list)
                            failed_urls.clear()
                            
                    # Update database with batch stats
                    self.db.update_memory_usage(self.crawl_id, current_memory)
                    
            logger.info(f"Batch processing completed: {processed} URLs processed")
            
        except Exception as e:
            logger.error(f"Critical error in batch processing: {str(e)}")
    
    async def process_url(self, crawler: AsyncWebCrawler, url: str, session_id: str):
        """Process a single URL and extract content and links"""
        try:
            if not self.memory_monitor.check_memory():
                logger.warning(f"Memory threshold reached, skipping URL: {url}")
                return

            # Check if URL was already processed successfully
            cursor = self.db.conn.cursor()
            cursor.execute('SELECT processed FROM pages WHERE url = ? AND status = ?', (url, 'success'))
            if cursor.fetchone():
                logger.debug(f"URL already processed successfully: {url}")
                return

            start_time = asyncio.get_event_loop().time()
            
            # Process URL with crawl4ai
            result = await crawler.arun(url, config=self.run_config, session_id=session_id)
            processing_time = asyncio.get_event_loop().time() - start_time
            
            # Update performance metrics
            self.performance_metrics['processing_times'].append(processing_time)
            
            if result.success:
                # Process content
                content = ''
                title = str(url)
                
                if hasattr(result, 'markdown_v2'):
                    content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else str(result.markdown_v2)
                    title = str(result.title) if hasattr(result, 'title') else title
                elif hasattr(result, 'cleaned_html'):
                    content = result.cleaned_html
                    
                # Save content to markdown
                filepath = None
                if content.strip():
                    filepath = self.processor.save_markdown(content, url)
                    
                # Process images
                images = []
                if hasattr(result, 'media') and 'images' in result.media:
                    images = await self.processor.save_images(result.media['images'], url)
                    
                # Save PDF if available
                pdf_path = None
                if hasattr(result, 'pdf') and result.pdf:
                    pdf_info = await self.processor.save_pdf(result.pdf, url)
                    if pdf_info:
                        pdf_path = pdf_info.get('filepath')
                        
                # Get metadata
                metadata = {
                    'content_length': len(content) if content else 0,
                    'processing_time': processing_time,
                    'image_count': len(images),
                    'has_pdf': bool(pdf_path)
                }
                if hasattr(result, 'metrics'):
                    metadata.update(result.metrics)

                # Extract and process links
                internal_links, external_links = self.get_links_from_result(result)
                
                # Add new URLs to discovered set
                new_urls = set()
                for link in internal_links:
                    if (link not in self.crawled_urls and 
                        link not in self.failed_urls and 
                        link not in self.discovered_urls and
                        link not in self.pending_urls):
                        new_urls.add(link)
                
                if new_urls:
                    logger.info(f"Found {len(new_urls)} new URLs on {url}")
                    self.discovered_urls.update(new_urls)
                    
                # Update link metrics
                link_metrics = {
                    'internal_links': len(internal_links),
                    'external_links': len(external_links),
                    'new_urls_found': len(new_urls)
                }
                metadata.update({'links': link_metrics})

                # Save to database with enhanced metadata and status tracking
                self.db.save_page_metadata(
                    url=url,
                    title=title,
                    filepath=str(filepath) if filepath else None,
                    images=images,
                    links={
                        'internal': list(internal_links),
                        'external': list(external_links)
                    },
                    status='success',
                    metadata=metadata,
                    screenshot_path=result.screenshot if hasattr(result, 'screenshot') else None,
                    pdf_path=pdf_path,
                    word_count=len(content.split()) if isinstance(content, str) else 0,
                    processed=True,
                    last_modified=datetime.utcnow()
                )

                self.db.update_crawl_stats(self.crawl_id, success=True)

            else:
                # Handle crawl failures with enhanced error tracking
                error_message = result.error_message if hasattr(result, 'error_message') else 'Unknown error'
                error_metadata = {
                    'error_type': 'crawl_error',
                    'processing_time': processing_time,
                    'retry_count': self.failed_urls.get(url, {}).get('retries', 0) + 1
                }
                
                if hasattr(result, 'status_code'):
                    error_metadata['status_code'] = result.status_code
                if hasattr(result, 'metrics'):
                    error_metadata.update(result.metrics)
                
                self.failed_urls[url] = {
                    'error': error_message,
                    'retries': error_metadata['retry_count'],
                    'last_attempt': datetime.utcnow()
                }
                
                # Save failure info to database
                self.db.save_page_metadata(
                    url=url,
                    title=url,
                    filepath=None,
                    images=[],
                    links={'internal': [], 'external': []},
                    status='failed',
                    error_message=error_message,
                    metadata=error_metadata,
                    processed=True,
                    last_modified=datetime.utcnow()
                )
                self.db.update_crawl_stats(self.crawl_id, success=False)
                
            # Mark URL as processed
            self.crawled_urls.add(url)
            
        except Exception as e:
            logger.error(f"Error processing {url}: {str(e)}")
            error_metadata = {
                'error_type': 'processing_error',
                'error_details': str(e),
                'retry_count': self.failed_urls.get(url, {}).get('retries', 0) + 1
            }
            
            self.failed_urls[url] = {
                'error': str(e),
                'retries': error_metadata['retry_count'],
                'last_attempt': datetime.utcnow()
            }
            
            # Save exception info to database
            self.db.save_page_metadata(
                url=url,
                title=url,
                filepath=None,
                images=[],
                links={'internal': [], 'external': []},
                status='error',
                error_message=str(e),
                metadata=error_metadata,
                processed=True,
                last_modified=datetime.utcnow()
            )
            self.db.update_crawl_stats(self.crawl_id, success=False)
            self.crawled_urls.add(url)