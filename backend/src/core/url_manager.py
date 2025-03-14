import logging
import asyncio
import aiohttp
import xml.etree.ElementTree as ET
from typing import List, Set, Dict, Optional, Tuple
from urllib.parse import urlparse, urljoin
from collections import defaultdict
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from bs4 import BeautifulSoup
import re
import time
from datetime import datetime

# Enhanced logging setup
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s - {%(funcName)s:%(lineno)d}',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('crawler.log')
    ]
)

class URLManager:
    """A sophisticated URL discovery and management system for web crawling.
    
    This class provides advanced functionality for discovering, tracking, and managing URLs
    during web crawling operations. It includes features such as:
    - Sitemap discovery and processing
    - Rate limiting and concurrency control
    - Performance monitoring and metrics
    - URL normalization and deduplication
    - Domain-based filtering
    
    Attributes:
        discovery_config (CrawlerRunConfig): Configuration for URL discovery behavior
        discovered_urls (Set[str]): Set of all discovered unique URLs
        processed_urls (Set[str]): Set of URLs that have been processed
        url_graph (Dict[str, Dict[str, List[str]]]): Graph structure of URL relationships
        base_domain (str): Base domain for the crawl operation
        processing_semaphore (asyncio.Semaphore): Global concurrency control
        domain_semaphores (Dict[str, asyncio.Semaphore]): Per-domain rate limiting
        performance_metrics (Dict): Detailed metrics about the crawling operation

    Example:
        >>> manager = URLManager()
        >>> urls = await manager.discover_urls("https://example.com", max_depth=2)
        >>> print(f"Discovered {len(urls)} URLs")
    """
    
    def __init__(self, discovery_config: Optional[CrawlerRunConfig] = None):
        logger.info("Initializing URLManager")
        base_config = CrawlerRunConfig(
            word_count_threshold=10,
            cache_mode=CacheMode.WRITE_ONLY,
            exclude_external_links=False,
            magic=True,
            simulate_user=True,
            scan_full_page=True
        )
        
        if discovery_config:
            config_dict = base_config.to_dict()
            config_dict.update(discovery_config.to_dict())
            self.discovery_config = CrawlerRunConfig(**config_dict)
            logger.debug(f"Using custom discovery config: {discovery_config}")
        else:
            self.discovery_config = base_config
            logger.debug("Using default discovery config")
            
        self.discovered_urls: Set[str] = set()
        self.processed_urls: Set[str] = set()
        self.url_graph: Dict[str, Dict[str, List[str]]] = {}
        self.base_domain: Optional[str] = None
        self.processing_semaphore = asyncio.Semaphore(10)
        self.domain_semaphores: Dict[str, asyncio.Semaphore] = defaultdict(
            lambda: asyncio.Semaphore(3)
        )
        self.performance_metrics = {
            'start_time': None,
            'end_time': None,
            'requests': defaultdict(int),
            'timings': defaultdict(list)
        }
        logger.info("URLManager initialized successfully")
        
    async def discover_single_url(self, url: str) -> List[str]:
        """Perform quick URL discovery for a single page without deep crawling.

        This method validates and processes a single URL without exploring
        sitemaps or performing deep crawling operations.

        Args:
            url (str): The URL to process and validate.

        Returns:
            List[str]: A list containing the normalized URL if valid,
                    empty list if validation fails.

        Raises:
            ValueError: If the URL format is invalid or domain cannot be extracted.

        Example:
            >>> urls = await manager.discover_single_url("https://example.com")
            >>> if urls:
            ...     print("URL is valid and accessible")
        """
        self.performance_metrics['start_time'] = datetime.now()
        logger.info(f"Starting single URL validation for: {url}")
        
        try:
            normalized_url = self._normalize_url(url)
            if not normalized_url:
                raise ValueError("Invalid URL format")
                
            self.base_domain = urlparse(normalized_url).netloc
            if not self.base_domain:
                raise ValueError(f"Could not extract domain from {url}")
                
            logger.info(f"Validated URL: {normalized_url}")
            
            # Initialize session to check URL accessibility
            timeout = aiohttp.ClientTimeout(total=10)
            connector = aiohttp.TCPConnector(limit=1, force_close=True)
            
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; SinglePageBot/1.0)'}
            ) as session:
                content, _, error = await self._fetch_with_aiohttp(normalized_url, session)
                if error:
                    raise ValueError(f"URL not accessible: {error}")
            
            # Set up minimal graph structure
            self.discovered_urls = {normalized_url}
            self.url_graph = {normalized_url: {'same_domain': [], 'external': []}}
            
            # Update metrics
            self.performance_metrics['end_time'] = datetime.now()
            duration = (self.performance_metrics['end_time'] - self.performance_metrics['start_time']).total_seconds()
            
            logger.info("Single URL Validation Complete:")
            logger.info(f"URL: {normalized_url}")
            logger.info(f"Time: {duration:.2f}s")
            
            return [normalized_url]
            
        except Exception as e:
            logger.error(f"Single URL validation failed for {url}: {str(e)}", exc_info=True)
            return []
        
    async def _fetch_with_aiohttp(self, url: str, session: aiohttp.ClientSession) -> Tuple[str, bool, str]:
        """Fetch URL content using aiohttp with built-in rate limiting.

        Args:
            url (str): The URL to fetch.
            session (aiohttp.ClientSession): The session to use for the request.

        Returns:
            Tuple[str, bool, str]: A tuple containing:
                - content (str): The fetched content
                - is_xml (bool): Whether the content is XML
                - error (str): Error message if any, empty string if successful

        Example:
            >>> async with aiohttp.ClientSession() as session:
            ...     content, is_xml, error = await manager._fetch_with_aiohttp(url, session)
            ...     if not error:
            ...         print(f"Fetched {len(content)} bytes")
        """
        domain = urlparse(url).netloc
        start_time = time.time()
        
        try:
            async with self.domain_semaphores[domain]:
                logger.debug(f"Fetching URL: {url}")
                async with self.processing_semaphore:
                    async with session.get(url, timeout=10) as response:
                        duration = time.time() - start_time
                        self.performance_metrics['timings']['fetch'].append(duration)
                        self.performance_metrics['requests']['total'] += 1
                        
                        status = response.status
                        if status == 200:
                            content = await response.text()
                            content_type = response.headers.get('Content-Type', '')
                            is_xml = 'xml' in content_type.lower()
                            logger.info(f"Successfully fetched {url} - Type: {'XML' if is_xml else 'HTML'} - Time: {duration:.2f}s")
                            return content, is_xml, ''
                        else:
                            logger.warning(f"Failed to fetch {url} - HTTP {status} - Time: {duration:.2f}s")
                            return '', False, f"HTTP {status}"
                            
        except asyncio.TimeoutError:
            duration = time.time() - start_time
            logger.error(f"Timeout fetching {url} after {duration:.2f}s")
            self.performance_metrics['requests']['timeouts'] += 1
            return '', False, 'Timeout'
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"Error fetching {url}: {str(e)} - Time: {duration:.2f}s")
            self.performance_metrics['requests']['errors'] += 1
            return '', False, str(e)
            
    async def _process_sitemap_url(self, url: str, session: aiohttp.ClientSession) -> Set[str]:
        """Process a sitemap URL and extract all contained URLs.

        Handles both XML sitemaps and sitemap indexes, including recursive
        processing of nested sitemaps.

        Args:
            url (str): The sitemap URL to process.
            session (aiohttp.ClientSession): The session to use for requests.

        Returns:
            Set[str]: Set of discovered and normalized URLs from the sitemap.

        Example:
            >>> async with aiohttp.ClientSession() as session:
            ...     urls = await manager._process_sitemap_url(
            ...         "https://example.com/sitemap.xml",
            ...         session
            ...     )
            ...     print(f"Found {len(urls)} URLs in sitemap")
        """
        logger.info(f"Processing sitemap: {url}")
        start_time = time.time()
        
        content, is_xml, error = await self._fetch_with_aiohttp(url, session)
        if error:
            logger.warning(f"Failed to fetch sitemap {url}: {error}")
            return set()
            
        urls = set()
        try:
            if is_xml:
                # Parse XML sitemap
                root = ET.fromstring(content)
                ns = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
                
                # Check if it's a sitemap index
                sitemaps = root.findall('.//ns:loc', ns)
                if sitemaps:
                    logger.info(f"Found sitemap index with {len(sitemaps)} sitemaps")
                    # Process each sitemap in parallel
                    tasks = []
                    for sitemap in sitemaps:
                        sitemap_url = sitemap.text
                        if sitemap_url:
                            logger.debug(f"Adding sitemap task: {sitemap_url}")
                            tasks.append(self._process_sitemap_url(sitemap_url, session))
                    if tasks:
                        results = await asyncio.gather(*tasks)
                        for result in results:
                            urls.update(result)
                
                # Get URLs from current sitemap
                locs = root.findall('.//ns:loc', ns)
                new_urls = {loc.text for loc in locs if loc.text}
                urls.update(new_urls)
                logger.info(f"Found {len(new_urls)} URLs in sitemap {url}")
            else:
                # Try parsing as HTML
                soup = BeautifulSoup(content, 'html.parser')
                links = soup.find_all('a', href=True)
                for link in links:
                    href = link['href']
                    if href:
                        full_url = urljoin(url, href)
                        if self._is_same_domain(full_url):
                            urls.add(full_url)
                logger.info(f"Found {len(urls)} URLs in HTML sitemap {url}")
                
        except ET.ParseError:
            logger.error(f"XML parsing error in sitemap {url}")
            self.performance_metrics['requests']['xml_errors'] += 1
        except Exception as e:
            logger.error(f"Error processing sitemap {url}: {str(e)}")
            self.performance_metrics['requests']['processing_errors'] += 1
            
        duration = time.time() - start_time
        self.performance_metrics['timings']['sitemap_processing'].append(duration)
        logger.debug(f"Sitemap processing completed - Time: {duration:.2f}s")
        
        return {url for url in urls if self._normalize_url(url)}
            
    async def _discover_sitemaps(self, base_url: str, session: aiohttp.ClientSession) -> Set[str]:
        """Discover and process all sitemaps"""
        logger.info(f"Starting sitemap discovery for {base_url}")
        start_time = time.time()
        
        parsed = urlparse(base_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        
        sitemap_patterns = [
            "/sitemap.xml",
            "/sitemap_index.xml",
            "/sitemap-index.xml",
            "/sitemaps/sitemap.xml",
            "/sitemap/sitemap.xml"
        ]
        
        # Try robots.txt first
        robots_url = urljoin(base, "/robots.txt")
        logger.info(f"Checking robots.txt at {robots_url}")
        content, _, error = await self._fetch_with_aiohttp(robots_url, session)
        if not error:
            sitemap_urls = re.findall(r'Sitemap:\s*(\S+)', content, re.IGNORECASE)
            if sitemap_urls:
                logger.info(f"Found {len(sitemap_urls)} sitemap URLs in robots.txt")
                sitemap_patterns.extend(sitemap_urls)
        else:
            logger.warning(f"Failed to fetch robots.txt: {error}")
        
        # Process all potential sitemaps
        all_urls = set()
        for pattern in sitemap_patterns:
            if not pattern.startswith('http'):
                sitemap_url = urljoin(base, pattern)
            else:
                sitemap_url = pattern
            logger.debug(f"Processing sitemap pattern: {sitemap_url}")
            urls = await self._process_sitemap_url(sitemap_url, session)
            all_urls.update(urls)
        
        duration = time.time() - start_time
        logger.info(f"Sitemap discovery completed - Found {len(all_urls)} URLs - Time: {duration:.2f}s")
        self.performance_metrics['timings']['sitemap_discovery'].append(duration)
        
        return all_urls
    
    def _is_same_domain(self, url: str) -> bool:
        """Check if a URL belongs to the base domain or its subdomains.

        Args:
            url (str): The URL to check.

        Returns:
            bool: True if the URL belongs to the base domain or its subdomains,
                False otherwise.

        Example:
            >>> manager.base_domain = "example.com"
            >>> manager._is_same_domain("https://sub.example.com")
            True
            >>> manager._is_same_domain("https://otherdomain.com")
            False
        """
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            base_domain = self.base_domain.lower()
            result = domain == base_domain or domain.endswith('.' + base_domain)
            logger.debug(f"Domain check - URL: {url} - Base: {base_domain} - Result: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in domain check for {url}: {str(e)}")
            return False
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize a URL by standardizing its format and removing fragments.

        This method:
        - Adds https:// if protocol is missing
        - Removes trailing slashes (except for root path)
        - Preserves query parameters
        - Removes fragments
        - Handles dict inputs with 'url' key

        Args:
            url (str | Dict): URL string or dictionary containing URL.

        Returns:
            Optional[str]: Normalized URL if valid, None if invalid.

        Example:
            >>> manager._normalize_url("example.com/path/")
            'https://example.com/path'
            >>> manager._normalize_url("https://example.com/path?q=1")
            'https://example.com/path?q=1'
        """
        try:
            if not url:
                return None
                
            if isinstance(url, dict) and 'url' in url:
                url = url['url']
            elif not isinstance(url, str):
                return None
                
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
                
            parsed = urlparse(url)
            if not parsed.netloc:
                return None
                
            path = parsed.path or '/'
            if path != '/' and path.endswith('/'):
                path = path[:-1]
                
            normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
            
            if parsed.query:
                normalized += f"?{parsed.query}"
                
            logger.debug(f"Normalized URL: {url} -> {normalized}")
            return normalized
            
        except Exception as e:
            logger.warning(f"Error normalizing URL {url}: {str(e)}")
            return None
    
    async def _extract_urls_from_html(self, url: str, html_content: str) -> Set[str]:
        """Extract and normalize URLs from HTML content.

        Args:
            url (str): The base URL for resolving relative links.
            html_content (str): The HTML content to parse.

        Returns:
            Set[str]: Set of normalized, unique URLs found in the HTML.

        Example:
            >>> urls = await manager._extract_urls_from_html(
            ...     "https://example.com",
            ...     "<a href='/page'>Link</a>"
            ... )
            >>> print(urls)
            {'https://example.com/page'}
        """
        start_time = time.time()
        urls = set()
        try:
            soup = BeautifulSoup(html_content, 'html.parser')
            links = soup.find_all('a', href=True)
            logger.debug(f"Found {len(links)} raw links in {url}")
            
            # Process and deduplicate URLs
            seen_urls = set()
            for link in links:
                href = link['href']
                if href and href.strip() and not href.startswith(('javascript:', 'mailto:', 'tel:')):
                    full_url = urljoin(url, href)
                    normalized = self._normalize_url(full_url)
                    if normalized:
                        seen_urls.add(normalized)
            urls.update(seen_urls)
                        
            duration = time.time() - start_time
            logger.info(f"Extracted {len(urls)} unique URLs from {url} - Time: {duration:.2f}s")
            self.performance_metrics['timings']['html_extraction'].append(duration)
            
        except Exception as e:
            logger.error(f"Error extracting URLs from {url}: {str(e)}")
            self.performance_metrics['requests']['extraction_errors'] += 1
            
        return urls
    
    async def _process_url_batch(
        self,
        urls: Set[str],
        session: aiohttp.ClientSession,
        current_depth: int
    ) -> Tuple[Set[str], Dict[str, Dict[str, List[str]]]]:
        """Process a batch of URLs in parallel"""
        logger.info(f"Processing batch of {len(urls)} URLs at depth {current_depth}")
        start_time = time.time()
        new_urls = set()
        url_data = {}
        
        async def process_single_url(url: str):
            if url in self.processed_urls:
                logger.debug(f"Skipping already processed URL: {url}")
                return
                
            url_start_time = time.time()
            content, _, error = await self._fetch_with_aiohttp(url, session)
            
            if error:
                logger.warning(f"Failed to fetch {url}: {error}")
                url_data[url] = {
                    'same_domain': [],
                    'external': [],
                    'error': error
                }
                return
                
            extracted_urls = await self._extract_urls_from_html(url, content)
            same_domain_urls = {
                url for url in extracted_urls 
                if self._is_same_domain(url)
            }
            
            url_data[url] = {
                'same_domain': list(same_domain_urls),
                'external': list(extracted_urls - same_domain_urls)
            }
            
            new_urls_count = len(same_domain_urls - self.discovered_urls)
            new_urls.update(same_domain_urls - self.discovered_urls)
            self.processed_urls.add(url)
            
            duration = time.time() - url_start_time
            logger.info(
                f"Processed {url} - Found {len(same_domain_urls)} same-domain URLs "
                f"({new_urls_count} new) - Time: {duration:.2f}s"
            )
        
        # Process URLs in parallel with controlled concurrency
        tasks = [process_single_url(url) for url in urls]
        await asyncio.gather(*tasks)
        
        batch_duration = time.time() - start_time
        self.performance_metrics['timings']['batch_processing'].append(batch_duration)
        logger.info(
            f"Completed batch processing - Found {len(new_urls)} new URLs - "
            f"Time: {batch_duration:.2f}s"
        )
        
        return new_urls, url_data
    
    async def discover_urls(self, base_url: str, max_depth: int = 3) -> List[str]:
        """Discover URLs by crawling from a base URL up to a specified depth.

        This is the main URL discovery method that:
        1. Processes sitemaps if available
        2. Performs breadth-first crawling up to max_depth
        3. Tracks performance metrics
        4. Handles rate limiting and concurrency

        Args:
            base_url (str): The starting URL for discovery.
            max_depth (int, optional): Maximum crawl depth. Defaults to 3.

        Returns:
            List[str]: List of all discovered unique URLs.

        Raises:
            ValueError: If base_url is invalid or domain cannot be extracted.

        Example:
            >>> urls = await manager.discover_urls(
            ...     "https://example.com",
            ...     max_depth=2
            ... )
            >>> print(f"Found {len(urls)} URLs up to depth 2")
        """
        self.performance_metrics['start_time'] = datetime.now()
        logger.info(f"Starting URL discovery from {base_url} with max_depth={max_depth}")
        
        try:
            base_url = self._normalize_url(base_url)
            if not base_url:
                raise ValueError("Invalid base URL")
                
            self.base_domain = urlparse(base_url).netloc
            if not self.base_domain:
                raise ValueError(f"Could not extract domain from {base_url}")
                
            logger.info(f"Using base domain: {self.base_domain}")
            
            timeout = aiohttp.ClientTimeout(total=30)
            connector = aiohttp.TCPConnector(limit=50, force_close=True)
            
            async with aiohttp.ClientSession(
                timeout=timeout,
                connector=connector,
                headers={'User-Agent': 'Mozilla/5.0 (compatible; URLDiscoveryBot/1.0)'}
            ) as session:
                # First, process sitemaps
                sitemap_urls = await self._discover_sitemaps(base_url, session)
                self.discovered_urls.update(sitemap_urls)
                logger.info(f"Found {len(sitemap_urls)} URLs from sitemaps")
                
                # Add base URL if not in sitemap
                if base_url not in self.discovered_urls:
                    self.discovered_urls.add(base_url)
                
                # Now process remaining URLs up to max_depth
                current_depth = 0
                urls_to_process = self.discovered_urls.copy()
                
                while urls_to_process and current_depth < max_depth:
                    logger.info(
                        f"Processing depth {current_depth + 1}/{max_depth} "
                        f"with {len(urls_to_process)} URLs"
                    )
                    
                    # Process current batch
                    new_urls, url_data = await self._process_url_batch(
                        urls_to_process,
                        session,
                        current_depth
                    )
                    
                    # Update tracking
                    self.url_graph.update(url_data)
                    self.discovered_urls.update(new_urls)
                    
                    # Prepare next batch
                    urls_to_process = new_urls
                    current_depth += 1
                    
                    logger.info(
                        f"Completed depth {current_depth}/{max_depth} - "
                        f"Total discovered: {len(self.discovered_urls)} URLs - "
                        f"New in this depth: {len(new_urls)}"
                    )
            
            # Record completion time and log final stats
            self.performance_metrics['end_time'] = datetime.now()
            total_time = (self.performance_metrics['end_time'] - self.performance_metrics['start_time']).total_seconds()
            
            # Calculate and log performance metrics
            avg_fetch_time = sum(self.performance_metrics['timings']['fetch']) / len(self.performance_metrics['timings']['fetch']) if self.performance_metrics['timings']['fetch'] else 0
            
            logger.info("URL Discovery Complete - Final Statistics:")
            logger.info(f"Total Time: {total_time:.2f}s")
            logger.info(f"Total URLs Discovered: {len(self.discovered_urls)}")
            logger.info(f"Total URLs Processed: {len(self.processed_urls)}")
            logger.info(f"Average Fetch Time: {avg_fetch_time:.2f}s")
            logger.info(f"Total Requests: {self.performance_metrics['requests']['total']}")
            logger.info(f"Failed Requests: {self.performance_metrics['requests']['errors']}")
            logger.info(f"Timeouts: {self.performance_metrics['requests']['timeouts']}")
            
            return list(self.discovered_urls)
            
        except Exception as e:
            logger.error(f"URL discovery failed: {str(e)}", exc_info=True)
            return []
    
    def get_unprocessed_urls(self) -> List[str]:
        """Get a list of discovered URLs that haven't been processed.

        Returns:
            List[str]: List of URLs that are discovered but not yet processed.

        Example:
            >>> pending = manager.get_unprocessed_urls()
            >>> print(f"{len(pending)} URLs pending processing")
        """
        unprocessed = list(self.discovered_urls - self.processed_urls)
        logger.info(f"Retrieved {len(unprocessed)} unprocessed URLs")
        return unprocessed
    
    def mark_as_processed(self, urls: List[str]):
        """Mark a list of URLs as processed.

        Args:
            urls (List[str]): List of URLs to mark as processed.

        Example:
            >>> manager.mark_as_processed(["https://example.com/page1"])
        """
        logger.info(f"Marking {len(urls)} URLs as processed")
        self.processed_urls.update(urls)
    
    def get_url_stats(self) -> Dict:
        """Get comprehensive statistics about URL discovery and processing.

        Returns:
            Dict: Dictionary containing:
                - total_discovered: Total number of unique URLs found
                - total_processed: Number of URLs that have been processed
                - pending: Number of URLs waiting to be processed
                - discovered_by_domain: Number of unique domains discovered
                - processing_time: Total time spent processing (if completed)
                - urls_per_second: Average processing rate

        Example:
            >>> stats = manager.get_url_stats()
            >>> print(f"Processed {stats['total_processed']} of {stats['total_discovered']} URLs")
            >>> if 'urls_per_second' in stats:
            ...     print(f"Average rate: {stats['urls_per_second']:.2f} URLs/second")
        """
        stats = {
            'total_discovered': len(self.discovered_urls),
            'total_processed': len(self.processed_urls),
            'pending': len(self.discovered_urls - self.processed_urls),
            'discovered_by_domain': len(set(urlparse(url).netloc for url in self.discovered_urls))
        }
        
        if self.performance_metrics['start_time'] and self.performance_metrics['end_time']:
            stats['processing_time'] = (self.performance_metrics['end_time'] - self.performance_metrics['start_time']).total_seconds()
            stats['urls_per_second'] = stats['total_discovered'] / stats['processing_time'] if stats['processing_time'] > 0 else 0
        
        logger.info("URL Statistics:")
        for key, value in stats.items():
            logger.info(f"{key}: {value}")
        
        return stats
