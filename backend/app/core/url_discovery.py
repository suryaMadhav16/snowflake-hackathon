import logging
import asyncio
from typing import List, Dict, Set, Optional
from urllib.parse import urljoin, urlparse
import re
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

logger = logging.getLogger(__name__)

class URLDiscoveryManager:
    """Manages URL discovery and validation"""
    
    def __init__(self, max_depth: int = 3, excluded_patterns: List[str] = None):
        self.max_depth = max_depth
        self.excluded_patterns = []
        if excluded_patterns:
            for pattern in excluded_patterns:
                try:
                    self.excluded_patterns.append(re.compile(pattern))
                except re.error as e:
                    logger.error(f"Invalid regex pattern '{pattern}': {str(e)}")
        
        self.discovered_urls: Set[str] = set()
        self._lock = asyncio.Lock()
    
    def _is_valid_url(self, url: str, base_domain: str) -> bool:
        """Check if URL is valid and matches base domain"""
        try:
            parsed = urlparse(url)
            if not parsed.netloc:
                return False
                
            # Check if URL matches base domain
            if parsed.netloc != base_domain:
                return False
            
            # Check exclusion patterns
            path = parsed.path.strip('/')
            for pattern in self.excluded_patterns:
                if pattern.search(path):
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error validating URL {url}: {str(e)}")
            return False
    
    def _extract_urls(self, html: str, base_url: str) -> Set[str]:
        """Extract URLs from HTML content"""
        urls = set()
        try:
            soup = BeautifulSoup(html, 'html.parser')
            base_domain = urlparse(base_url).netloc
            
            # Process links
            for link in soup.find_all(['a', 'area'], href=True):
                href = link['href']
                full_url = urljoin(base_url, href)
                
                if self._is_valid_url(full_url, base_domain):
                    urls.add(full_url)
            
            # Process other resources (images, scripts, etc.)
            for tag in soup.find_all(['img', 'script', 'link'], src=True):
                src = tag['src']
                full_url = urljoin(base_url, src)
                
                if self._is_valid_url(full_url, base_domain):
                    urls.add(full_url)
            
            return urls
            
        except Exception as e:
            logger.error(f"Error extracting URLs from {base_url}: {str(e)}")
            return urls
    
    async def discover_urls(
        self,
        start_url: str,
        mode: str = "full"
    ) -> Dict:
        """Discover URLs starting from given URL"""
        try:
            # Configure crawler
            browser_config = BrowserConfig(
                headless=True,
                browser_type="chromium",
                user_agent_mode="random"
            )
            
            crawler_config = CrawlerRunConfig(
                magic=True,
                simulate_user=True,
                cache_mode=CacheMode.ENABLED,
                word_count_threshold=10  # Minimal processing for discovery
            )
            
            # Initialize discovery
            self.discovered_urls = set()
            current_depth = 0
            current_level = {start_url}
            url_graph = {
                "nodes": [],
                "links": []
            }
            
            base_domain = urlparse(start_url).netloc
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                while current_level and current_depth < self.max_depth:
                    next_level = set()
                    
                    # Process current level
                    for url in current_level:
                        if url in self.discovered_urls:
                            continue
                        
                        try:
                            # Crawl URL
                            result = await crawler.arun(
                                url=url,
                                config=crawler_config
                            )
                            
                            if result.success:
                                # Add to discovered URLs
                                async with self._lock:
                                    self.discovered_urls.add(url)
                                
                                # Add to graph
                                url_graph["nodes"].append({
                                    "id": url,
                                    "depth": current_depth
                                })
                                
                                # Extract and validate new URLs
                                new_urls = self._extract_urls(result.html, url)
                                for new_url in new_urls:
                                    if new_url not in self.discovered_urls:
                                        next_level.add(new_url)
                                        url_graph["links"].append({
                                            "source": url,
                                            "target": new_url
                                        })
                            
                        except Exception as e:
                            logger.error(f"Error processing {url}: {str(e)}")
                            continue
                    
                    # Move to next level
                    current_level = next_level
                    current_depth += 1
                    
                    # Break early if in quick mode
                    if mode == "quick" and len(self.discovered_urls) >= 100:
                        break
            
            return {
                "urls": list(self.discovered_urls),
                "total": len(self.discovered_urls),
                "max_depth": current_depth,
                "graph": url_graph
            }
            
        except Exception as e:
            logger.error(f"URL discovery error: {str(e)}")
            raise