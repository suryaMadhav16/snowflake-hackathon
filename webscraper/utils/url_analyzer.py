import re
from urllib.parse import urlparse, urljoin
from typing import Dict, List, Set, Optional, Tuple
import logging
from collections import defaultdict
from datetime import datetime
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
import asyncio
import json

logger = logging.getLogger(__name__)

class URLPattern:
    """Represents a URL pattern node in the pattern tree for Crawl4AI analysis"""
    
    def __init__(self, segment: str, parent: Optional['URLPattern'] = None):
        self.segment = segment
        self.parent = parent
        self.children: Dict[str, URLPattern] = {}
        self.urls: Set[str] = set()
        self.pattern_id = None
        self.metadata = {
            'count': 0,
            'success_rate': 100.0,
            'last_crawl': None,
            'content_types': defaultdict(int),
            'dynamic_segments': [],
            'avg_load_time': 0.0,
            'avg_word_count': 0,
            'resource_counts': {
                'images': 0,
                'scripts': 0,
                'styles': 0,
                'links': 0
            },
            'performance_metrics': {
                'memory_usage': [],
                'load_times': [],
                'success_counts': 0,
                'failure_counts': 0
            }
        }
    
    @property
    def full_pattern(self) -> str:
        """Get the full pattern path from root"""
        if self.parent and self.parent.segment != '/':
            return f"{self.parent.full_pattern}/{self.segment}".replace('//', '/')
        return f"/{self.segment}".replace('//', '/')
    
    def get_pattern_dict(self) -> Dict:
        """Convert pattern node to dictionary with enhanced metrics"""
        success_rate = 0
        if (total := (self.metadata['performance_metrics']['success_counts'] + 
                     self.metadata['performance_metrics']['failure_counts'])) > 0:
            success_rate = (self.metadata['performance_metrics']['success_counts'] / total) * 100
            
        return {
            'id': self.pattern_id,
            'pattern': self.full_pattern,
            'metrics': {
                'count': self.metadata['count'],
                'success_rate': success_rate,
                'avg_load_time': sum(self.metadata['performance_metrics']['load_times']) / 
                               len(self.metadata['performance_metrics']['load_times'])
                               if self.metadata['performance_metrics']['load_times'] else 0,
                'avg_word_count': self.metadata['avg_word_count'],
                'avg_memory_usage': sum(self.metadata['performance_metrics']['memory_usage']) /
                                  len(self.metadata['performance_metrics']['memory_usage'])
                                  if self.metadata['performance_metrics']['memory_usage'] else 0
            },
            'resources': self.metadata['resource_counts'],
            'contentTypes': dict(self.metadata['content_types']),
            'dynamicSegments': self.metadata['dynamic_segments'],
            'lastCrawl': self.metadata['last_crawl'],
            'examples': list(self.urls)[:5],
            'children': [child.get_pattern_dict() for child in self.children.values()]
        }

    def update_metrics(self, crawl_result, performance_metrics: Dict = None) -> None:
        """Update pattern metrics from Crawl4AI result"""
        self.metadata['count'] += 1
        
        # Update success/failure counts
        if crawl_result.success:
            self.metadata['performance_metrics']['success_counts'] += 1
        else:
            self.metadata['performance_metrics']['failure_counts'] += 1
        
        # Update performance metrics
        if performance_metrics:
            self.metadata['performance_metrics']['memory_usage'].append(
                performance_metrics.get('memory_usage', 0)
            )
            self.metadata['performance_metrics']['load_times'].append(
                performance_metrics.get('load_time', 0)
            )
            
            # Keep only last 100 measurements
            max_history = 100
            self.metadata['performance_metrics']['memory_usage'] = \
                self.metadata['performance_metrics']['memory_usage'][-max_history:]
            self.metadata['performance_metrics']['load_times'] = \
                self.metadata['performance_metrics']['load_times'][-max_history:]
        
        # Update content metrics
        if crawl_result.markdown_v2:
            current_word_count = self.metadata['avg_word_count'] * (self.metadata['count'] - 1)
            new_word_count = len(crawl_result.markdown_v2.get('content', '').split())
            self.metadata['avg_word_count'] = (current_word_count + new_word_count) / self.metadata['count']
        
        # Update resource counts
        if crawl_result.media:
            self.metadata['resource_counts']['images'] += len(crawl_result.media.get('images', []))
            self.metadata['resource_counts']['scripts'] += len(crawl_result.media.get('scripts', []))
            self.metadata['resource_counts']['styles'] += len(crawl_result.media.get('styles', []))
        
        if crawl_result.links:
            self.metadata['resource_counts']['links'] = \
                len(crawl_result.links.get('internal', [])) + len(crawl_result.links.get('external', []))
        
        # Update last crawl timestamp
        self.metadata['last_crawl'] = datetime.now().isoformat()

class URLPatternAnalyzer:
    """Analyzes URLs using Crawl4AI for pattern detection and analysis"""
    
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.root = URLPattern('/')
        self.pattern_id_counter = 0
        self.patterns_by_id: Dict[str, URLPattern] = {}
        
        # Crawl4AI configuration
        self.browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            viewport_width=1080,
            viewport_height=800
        )
        
        self.run_config = CrawlerRunConfig(
            cache_mode="ENABLED",
            word_count_threshold=10,
            magic=True,
            simulate_user=True
        )
    
    def _generate_pattern_id(self) -> str:
        """Generate unique pattern ID"""
        self.pattern_id_counter += 1
        return f"pattern_{self.pattern_id_counter}"
    
    def _extract_dynamic_segments(self, segment: str) -> List[str]:
        """Extract and classify dynamic URL segments"""
        patterns = [
            (r'^\d+$', 'numeric_id'),
            (r'^[a-f0-9]{8,}$', 'hash'),
            (r'^\d{4}-\d{2}-\d{2}$', 'date'),
            (r'^(p|post|article|product)-[\w-]+$', 'slug'),
            (r'^v\d+$', 'version'),
            (r'^page-\d+$', 'pagination'),
            (r'^[a-z]{2}(-[A-Z]{2})?$', 'locale')
        ]
        
        dynamic_segments = []
        for pattern, segment_type in patterns:
            if re.match(pattern, segment):
                dynamic_segments.append(segment_type)
        return dynamic_segments

    def _normalize_segment(self, segment: str) -> str:
        """Normalize URL segment to a pattern"""
        # Handle numeric IDs
        if re.match(r'^\d+$', segment):
            return '{n}'
        
        # Handle UUIDs and hashes
        if re.match(r'^[a-f0-9]{8,}$', segment):
            return '{id}'
        
        # Handle dates
        if re.match(r'^\d{4}-\d{2}-\d{2}$', segment):
            return '{date}'
        
        # Handle common slug patterns
        if re.match(r'^(p|post|article|product)-[\w-]+$', segment):
            return '{slug}'
        
        # Handle versioning
        if re.match(r'^v\d+$', segment):
            return '{version}'
        
        # Handle pagination
        if re.match(r'^page-\d+$', segment):
            return '{page}'
        
        # Handle locales
        if re.match(r'^[a-z]{2}(-[A-Z]{2})?$', segment):
            return '{locale}'
        
        return segment

    async def _analyze_url(self, url: str, crawler: AsyncWebCrawler) -> Tuple[bool, Dict]:
        """Analyze a single URL using Crawl4AI"""
        try:
            start_time = datetime.now()
            result = await crawler.arun(url, config=self.run_config)
            load_time = (datetime.now() - start_time).total_seconds()
            
            performance_metrics = {
                'load_time': load_time,
                'memory_usage': result.metrics.get('memory_usage', 0) if hasattr(result, 'metrics') else 0
            }
            
            return True, performance_metrics
            
        except Exception as e:
            logger.error(f"Error analyzing URL {url}: {str(e)}")
            return False, {}

    def _add_url_to_tree(self, url: str, performance_metrics: Dict = None):
        """Add URL to pattern tree with performance data"""
        if not url.startswith(('http://', 'https://')):
            url = urljoin(self.base_url, url)
        
        parsed = urlparse(url)
        if parsed.netloc != self.domain:
            return
        
        segments = [s for s in parsed.path.split('/') if s]
        current = self.root
        
        for segment in segments:
            normalized = self._normalize_segment(segment)
            if normalized not in current.children:
                pattern = URLPattern(normalized, parent=current)
                pattern.pattern_id = self._generate_pattern_id()
                pattern.metadata['dynamic_segments'] = self._extract_dynamic_segments(segment)
                current.children[normalized] = pattern
                self.patterns_by_id[pattern.pattern_id] = pattern
            
            current = current.children[normalized]
            current.urls.add(url)
            
            if performance_metrics:
                current.metadata['performance_metrics']['memory_usage'].append(
                    performance_metrics.get('memory_usage', 0)
                )
                current.metadata['performance_metrics']['load_times'].append(
                    performance_metrics.get('load_time', 0)
                )

    async def analyze_sitemap(self) -> Dict:
        """Analyze sitemap URLs using Crawl4AI"""
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            sitemap_url = urljoin(self.base_url, '/sitemap.xml')
            
            try:
                result = await crawler.arun(sitemap_url, config=self.run_config)
                
                if result.success and result.links:
                    # Process all discovered URLs
                    for url in result.links.get('internal', []):
                        success, metrics = await self._analyze_url(url, crawler)
                        if success:
                            self._add_url_to_tree(url, metrics)
                    
                    return self.root.get_pattern_dict()
                    
                return {'error': 'Failed to process sitemap'}
                
            except Exception as e:
                logger.error(f"Error analyzing sitemap: {str(e)}")
                return {'error': str(e)}

    def filter_urls(self, selected_pattern_ids: Set[str]) -> List[str]:
        """Get all URLs matching selected pattern IDs"""
        filtered_urls = set()
        for pattern_id in selected_pattern_ids:
            if pattern_id in self.patterns_by_id:
                filtered_urls.update(self.patterns_by_id[pattern_id].urls)
        return list(filtered_urls)

    def get_pattern_metrics(self, pattern_id: str) -> Dict:
        """Get detailed metrics for a specific pattern"""
        if pattern_id in self.patterns_by_id:
            pattern = self.patterns_by_id[pattern_id]
            return {
                'pattern': pattern.full_pattern,
                'metrics': pattern.get_pattern_dict()['metrics'],
                'resources': pattern.metadata['resource_counts'],
                'performance': {
                    'memory_usage_trend': pattern.metadata['performance_metrics']['memory_usage'],
                    'load_times_trend': pattern.metadata['performance_metrics']['load_times'],
                    'success_rate': (pattern.metadata['performance_metrics']['success_counts'] /
                                   max(pattern.metadata['count'], 1) * 100)
                }
            }
        return {}

    def get_dynamic_segments_summary(self) -> Dict[str, int]:
        """Get summary of dynamic segment types across all patterns"""
        segment_counts = defaultdict(int)
        for pattern in self.patterns_by_id.values():
            for segment_type in pattern.metadata['dynamic_segments']:
                segment_counts[segment_type] += 1
        return dict(segment_counts)