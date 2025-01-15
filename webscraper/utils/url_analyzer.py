import re
from urllib.parse import urlparse, urljoin
from typing import Dict, List, Set
import logging
from collections import defaultdict
import aiohttp
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

class URLPatternAnalyzer:
    """Analyzes URLs to find common patterns"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.patterns = defaultdict(set)
        self.pattern_counts = defaultdict(int)
        
    def _extract_pattern(self, url: str) -> str:
        """Extract pattern from URL"""
        parsed = urlparse(url)
        path = parsed.path
        
        # Replace numeric segments with {n}
        path = re.sub(r'/\d+', '/{n}', path)
        
        # Replace UUIDs/hashes with {id}
        path = re.sub(r'/[a-f0-9]{8,}', '/{id}', path)
        
        # Replace date patterns with {date}
        path = re.sub(r'/\d{4}/\d{2}/\d{2}', '/{date}', path)
        
        # Replace common dynamic segments
        path = re.sub(r'/p/[^/]+', '/p/{slug}', path)
        path = re.sub(r'/post/[^/]+', '/post/{slug}', path)
        path = re.sub(r'/article/[^/]+', '/article/{slug}', path)
        
        return path if path else '/'

    def add_url(self, url: str):
        """Add URL to pattern analysis"""
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.base_url, url)
            
        if urlparse(url).netloc != self.domain:
            return
            
        pattern = self._extract_pattern(url)
        self.patterns[pattern].add(url)
        self.pattern_counts[pattern] += 1
    
    async def analyze_sitemap(self) -> Dict[str, List[str]]:
        """Analyze sitemap URLs and return patterns"""
        sitemap_url = urljoin(self.base_url, '/sitemap.xml')
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(sitemap_url) as response:
                    if response.status == 200:
                        text = await response.text()
                        soup = BeautifulSoup(text, 'xml')
                        
                        # Process standard sitemap
                        for loc in soup.find_all('loc'):
                            url = loc.text.strip()
                            self.add_url(url)
                        
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
                                            self.add_url(url)
                                            
                        logger.info(f"Found {len(self.patterns)} distinct URL patterns")
                        
                        return {
                            pattern: {
                                'urls': list(urls),
                                'count': self.pattern_counts[pattern],
                                'example': next(iter(urls))
                            }
                            for pattern, urls in self.patterns.items()
                        }
                        
        except Exception as e:
            logger.error(f"Error analyzing sitemap: {str(e)}")
            return {}

    def filter_urls(self, selected_patterns: Set[str]) -> List[str]:
        """Get all URLs matching selected patterns"""
        filtered_urls = []
        for pattern in selected_patterns:
            filtered_urls.extend(self.patterns[pattern])
        return filtered_urls
