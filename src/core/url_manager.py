import logging
from typing import List, Set, Dict, Optional
from urllib.parse import urlparse, urljoin
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode

logger = logging.getLogger(__name__)

class URLManager:
    """Manages URL discovery and tracking for the web crawler"""
    
    def __init__(self, discovery_config: Optional[CrawlerRunConfig] = None):
        base_config = CrawlerRunConfig(
            word_count_threshold=10,
            cache_mode=CacheMode.WRITE_ONLY,
            exclude_external_links=False,  # Allow external links initially
            magic=True,
            simulate_user=True,
            scan_full_page=True,
            wait_until="networkidle",
            delay_before_return_html=2.0,
            # Disable media saving during discovery
            screenshot=False,
            pdf=False,
            exclude_external_images=True
        )
        
        if discovery_config:
            config_dict = base_config.to_dict()
            config_dict.update(discovery_config.to_dict())
            self.discovery_config = CrawlerRunConfig(**config_dict)
        else:
            self.discovery_config = base_config
            
        self.discovered_urls: Set[str] = set()
        self.processed_urls: Set[str] = set()
        self.url_graph: Dict[str, Dict[str, List[str]]] = {}
        self.base_domain: Optional[str] = None
        
    def _is_same_domain(self, url: str) -> bool:
        """Check if URL belongs to the same domain"""
        try:
            parsed = urlparse(url)
            domain = parsed.netloc.lower()
            base_domain = self.base_domain.lower()
            # Handle subdomains
            return domain == base_domain or domain.endswith('.' + base_domain)
        except:
            return False
    
    def _normalize_url(self, url: str) -> Optional[str]:
        """Normalize URL by removing fragments and standardizing format"""
        try:
            if not url:
                return None
                
            # Handle dictionary case
            if isinstance(url, dict) and 'url' in url:
                url = url['url']
            elif not isinstance(url, str):
                return None
                
            # Add scheme if missing
            if not url.startswith(('http://', 'https://')):
                url = 'https://' + url
                
            parsed = urlparse(url)
            # Skip invalid URLs
            if not parsed.netloc:
                return None
                
            # Remove fragments and normalize path
            path = parsed.path or '/'
            if path != '/' and path.endswith('/'):
                path = path[:-1]
                
            # Reconstruct URL
            normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
            
            # Add query parameters if present
            if parsed.query:
                normalized += f"?{parsed.query}"
                
            return normalized
            
        except Exception as e:
            logger.warning(f"Error normalizing URL {url}: {str(e)}")
            return None
    
    def _extract_urls_from_result(self, result, base_url: str) -> Set[str]:
        """Extract and normalize URLs from crawl result"""
        urls = set()
        try:
            if not result.links:
                logger.warning(f"No links found in result for {base_url}")
                return urls
            
            # Debug log the links structure
            logger.debug(f"Links structure: {result.links}")
            
            # Extract links from the result
            all_links = []
            
            if isinstance(result.links, dict):
                # Get all links from internal and external collections
                internal_links = result.links.get('internal', [])
                external_links = result.links.get('external', [])
                
                # Convert to list if they're not already
                if not isinstance(internal_links, list):
                    internal_links = [internal_links] if internal_links else []
                if not isinstance(external_links, list):
                    external_links = [external_links] if external_links else []
                
                all_links.extend(internal_links)
                all_links.extend(external_links)
            elif isinstance(result.links, list):
                all_links = result.links
            else:
                logger.warning(f"Unexpected links format: {type(result.links)}")
                return urls
            
            # Process each URL
            for url_item in all_links:
                try:
                    # Handle both string and dictionary formats
                    if isinstance(url_item, dict):
                        url = url_item.get('url') or url_item.get('href')
                    else:
                        url = str(url_item)
                    
                    if not url:
                        continue
                    
                    # Convert relative URLs to absolute
                    if not url.startswith(('http://', 'https://')):
                        url = urljoin(base_url, url)
                    
                    normalized = self._normalize_url(url)
                    if normalized:
                        urls.add(normalized)
                    
                except Exception as e:
                    logger.warning(f"Error processing URL {url_item}: {str(e)}")
            
            logger.debug(f"Extracted {len(urls)} unique URLs from {base_url}")
            
        except Exception as e:
            logger.error(f"Error extracting URLs from {base_url}: {str(e)}")
        
        return urls
    
    async def discover_urls(self, base_url: str, max_depth: int = 3) -> List[str]:
        """Discover URLs starting from base_url up to max_depth"""
        try:
            # Normalize and validate base URL
            base_url = self._normalize_url(base_url)
            if not base_url:
                raise ValueError("Invalid base URL")
                
            self.base_domain = urlparse(base_url).netloc
            if not self.base_domain:
                raise ValueError(f"Could not extract domain from {base_url}")
                
            logger.info(f"Starting URL discovery from {base_url} (domain: {self.base_domain})")
            
            urls_to_process = {base_url}
            current_depth = 0
            
            async with AsyncWebCrawler() as crawler:
                while urls_to_process and current_depth < max_depth:
                    current_urls = urls_to_process.copy()
                    urls_to_process.clear()
                    
                    logger.info(f"Processing depth {current_depth + 1} with {len(current_urls)} URLs")
                    
                    for url in current_urls:
                        if url in self.discovered_urls:
                            continue
                            
                        try:
                            logger.info(f"Fetching {url}")
                            result = await crawler.arun(url, self.discovery_config)
                            
                            if not result.success:
                                logger.warning(f"Failed to fetch {url}: {result.error_message}")
                                continue
                            
                            # Extract and filter URLs
                            all_urls = self._extract_urls_from_result(result, url)
                            same_domain_urls = {
                                url for url in all_urls 
                                if self._is_same_domain(url)
                            }
                            
                            # Update tracking
                            self.url_graph[url] = {
                                'same_domain': list(same_domain_urls),
                                'external': list(all_urls - same_domain_urls)
                            }
                            self.discovered_urls.add(url)
                            
                            # Add new URLs to process
                            new_urls = same_domain_urls - self.discovered_urls
                            urls_to_process.update(new_urls)
                            
                            logger.info(
                                f"Found {len(new_urls)} new URLs at {url} "
                                f"(same domain: {len(same_domain_urls)}, "
                                f"total: {len(all_urls)})"
                            )
                            
                        except Exception as e:
                            logger.error(f"Error processing {url}: {str(e)}")
                    
                    current_depth += 1
                    logger.info(
                        f"Completed depth {current_depth}/{max_depth}, "
                        f"discovered {len(self.discovered_urls)} URLs"
                    )
            
            return list(self.discovered_urls)
            
        except Exception as e:
            logger.error(f"URL discovery failed: {str(e)}")
            return []
    
    def get_unprocessed_urls(self) -> List[str]:
        """Get list of discovered URLs that haven't been processed yet"""
        return list(self.discovered_urls - self.processed_urls)
    
    def mark_as_processed(self, urls: List[str]):
        """Mark URLs as processed"""
        self.processed_urls.update(urls)
    
    def get_url_stats(self) -> Dict:
        """Get statistics about discovered and processed URLs"""
        return {
            'total_discovered': len(self.discovered_urls),
            'total_processed': len(self.processed_urls),
            'pending': len(self.discovered_urls - self.processed_urls),
            'discovered_by_domain': len(set(urlparse(url).netloc for url in self.discovered_urls))
        }