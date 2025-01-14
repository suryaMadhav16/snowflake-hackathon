import asyncio
import os
import time
import aiohttp
import logging
from urllib.parse import urlparse, urljoin, urldefrag
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from dataclasses import dataclass, field
from typing import Set, Dict, List, Optional
from collections import defaultdict
import xml.etree.ElementTree as ET
from datetime import datetime
from tqdm import tqdm
import re

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlStats:
    """Track crawling statistics"""
    start_time: float = field(default_factory=time.time)
    pages_crawled: int = 0
    failed_urls: Dict[str, str] = field(default_factory=dict)
    status_codes: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    content_types: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    avg_page_size: float = 0
    total_size: int = 0
    depth_distribution: Dict[int, int] = field(default_factory=lambda: defaultdict(int))
    saved_files: List[str] = field(default_factory=list)

    def add_page(self, url: str, status: int, size: int, depth: int):
        """Record stats for a crawled page"""
        self.pages_crawled += 1
        self.status_codes[status] += 1
        self.total_size += size
        self.avg_page_size = self.total_size / self.pages_crawled
        self.depth_distribution[depth] += 1

    def add_saved_file(self, filepath: str):
        """Record a successfully saved file"""
        self.saved_files.append(filepath)

    def add_failure(self, url: str, error: str):
        """Record a failed crawl attempt"""
        self.failed_urls[url] = error

    def generate_report(self) -> str:
        """Generate a human-readable report of crawl statistics"""
        duration = time.time() - self.start_time
        report = [
            "Crawl Statistics Report",
            "=====================",
            f"Duration: {duration:.2f} seconds",
            f"Pages Crawled: {self.pages_crawled}",
            f"Files Saved: {len(self.saved_files)}",
            f"Average Page Size: {self.avg_page_size / 1024:.2f} KB",
            f"Total Size: {self.total_size / (1024*1024):.2f} MB",
            "\nStatus Code Distribution:",
            *[f"  {code}: {count}" for code, count in self.status_codes.items()],
            "\nDepth Distribution:",
            *[f"  Level {depth}: {count} pages" for depth, count in sorted(self.depth_distribution.items())],
            "\nFailed URLs:",
            *[f"  {url}: {error}" for url, error in self.failed_urls.items()],
            "\nSaved Files:",
            *[f"  {filepath}" for filepath in self.saved_files]
        ]
        return "\n".join(report)

class SitemapParser:
    """Parse and validate against website sitemaps"""
    def __init__(self, session: aiohttp.ClientSession, base_path: str = "/en/"):
        self.session = session
        self.urls: Set[str] = set()
        self.base_path = base_path

    async def fetch_sitemap(self, base_url: str):
        """Fetch and parse sitemap.xml"""
        try:
            sitemap_url = urljoin(base_url, '/sitemap.xml')
            async with self.session.get(sitemap_url) as response:
                if response.status == 200:
                    content = await response.text()
                    self.parse_sitemap_content(content)
                    logger.info(f"Found {len(self.urls)} URLs in sitemap")
                else:
                    logger.warning(f"Sitemap not found at {sitemap_url}")
        except Exception as e:
            logger.error(f"Error fetching sitemap: {str(e)}")

    def parse_sitemap_content(self, content: str):
        """Parse sitemap XML content and filter for /en/ URLs"""
        try:
            root = ET.fromstring(content)
            namespaces = {'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
            
            # Parse URLs from regular sitemap
            for url in root.findall('.//sm:url/sm:loc', namespaces):
                if self.base_path in url.text:
                    self.urls.add(url.text)
                
            # Parse URLs from sitemap index
            for sitemap in root.findall('.//sm:sitemap/sm:loc', namespaces):
                if self.base_path in sitemap.text:
                    asyncio.create_task(self.fetch_sitemap(sitemap.text))
                
        except ET.ParseError as e:
            logger.error(f"Error parsing sitemap XML: {str(e)}")

class ImprovedWebCrawler:
    """Enhanced web crawler with validation and comprehensive statistics"""
    
    def __init__(self, max_depth: int = 5, max_pages: int = 1000, 
                 delay: float = 1.0, max_retries: int = 3, base_path: str = "/en/"):
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.delay = delay
        self.max_retries = max_retries
        self.base_path = base_path
        
        self.visited: Set[str] = set()
        self.queued: Set[str] = set()
        self.stats = CrawlStats()
        self.base_domain: Optional[str] = None
        
        self.crawler: Optional[AsyncWebCrawler] = None
        self.sitemap: Optional[SitemapParser] = None
        self.progress_bar: Optional[tqdm] = None

    def normalize_url(self, url: str) -> str:
        """Normalize URL to prevent duplicates"""
        parsed = urlparse(url)
        # Remove fragments and normalize case
        url = urldefrag(url)[0].lower()
        
        # Normalize domain
        if parsed.netloc.startswith('www.'):
            url = url.replace(parsed.netloc, parsed.netloc[4:])
            
        # Ensure consistent trailing slashes for paths
        if parsed.path and not parsed.path.endswith('/'):
            url = f"{url}/"
            
        return url

    def is_valid_url(self, url: str) -> bool:
        """Check if URL should be crawled"""
        if not url:
            return False
            
        parsed = urlparse(url)
        
        # Skip non-HTTP(S) URLs
        if parsed.scheme not in ('http', 'https'):
            return False
            
        # Skip external domains
        if self.base_domain and parsed.netloc != self.base_domain:
            return False
            
        # Only crawl URLs containing base_path (e.g., /en/)
        if self.base_path not in parsed.path:
            return False
            
        # Skip common non-content extensions
        skip_extensions = {'.pdf', '.jpg', '.png', '.gif', '.css', '.js', '.woff', '.woff2', '.ttf'}
        if any(parsed.path.endswith(ext) for ext in skip_extensions):
            return False
            
        return True

    def url_to_filepath(self, url: str, output_dir: str) -> str:
        """Convert URL to a file path for saving"""
        parsed = urlparse(url)
        path = parsed.path.strip('/')
        
        # Handle the case where there's no path
        if not path:
            path = 'index'
            
        # Replace remaining slashes with underscores
        filepath = path.replace('/', '_')
        
        # Add .md extension if not present
        if not filepath.endswith('.md'):
            filepath += '.md'
            
        return os.path.join(output_dir, filepath)

    async def save_page_content(self, url: str, result, output_dir: str):
        """Save page content to file as markdown"""
        try:
            filepath = self.url_to_filepath(url, output_dir)
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                # Write title as h1 header
                if result.metadata.get('title'):
                    f.write(f"# {result.metadata['title']}\n\n")
                
                # Write the markdown content
                f.write(result.markdown)
                
                # If there are images, add them to the markdown
                if result.media and result.media.get("images"):
                    f.write("\n\n## Images\n\n")
                    for img in result.media["images"]:
                        f.write(f"![{img.get('alt', '')}]({img.get('src', '')})\n\n")
            
            self.stats.add_saved_file(filepath)
            logger.debug(f"Saved markdown content to {filepath}")
            
        except Exception as e:
            logger.error(f"Error saving content for {url}: {str(e)}")
            self.stats.add_failure(url, f"Failed to save content: {str(e)}")

    async def crawl_page(self, url: str, depth: int, output_dir: str) -> Set[str]:
        """Crawl a single page and extract links"""
        if depth > self.max_depth or len(self.visited) >= self.max_pages:
            return set()

        normalized_url = self.normalize_url(url)
        if normalized_url in self.visited:
            return set()

        # Implement politeness delay
        await asyncio.sleep(self.delay)

        try:
            config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS
            )
            result = await self.crawler.arun(url=url, config=config)

            if result.success:
                self.visited.add(normalized_url)
                
                # Record statistics
                self.stats.add_page(
                    url=normalized_url,
                    status=result.status_code,
                    size=len(result.html),
                    depth=depth
                )

                # Save the page content as markdown
                await self.save_page_content(url, result, output_dir)

                # Update progress bar
                if self.progress_bar:
                    self.progress_bar.update(1)

                # Extract and return valid links
                new_links = set()
                for link_type in ['internal', 'external']:
                    for link in result.links.get(link_type, []):
                        link_url = link.get('href', '')
                        if self.is_valid_url(link_url):
                            new_links.add(link_url)

                return new_links
            else:
                self.stats.add_failure(url, f"Crawl failed with status {result.status_code}")
                return set()

        except Exception as e:
            self.stats.add_failure(url, str(e))
            logger.error(f"Error crawling {url}: {str(e)}")
            return set()

    async def validate_coverage(self) -> Dict:
        """Validate crawl coverage against sitemap"""
        if not self.sitemap:
            return {"error": "No sitemap available"}

        crawled_urls = {self.normalize_url(url) for url in self.visited}
        sitemap_urls = {self.normalize_url(url) for url in self.sitemap.urls}

        return {
            "total_crawled": len(crawled_urls),
            "total_in_sitemap": len(sitemap_urls),
            "missed_urls": list(sitemap_urls - crawled_urls),
            "extra_urls": list(crawled_urls - sitemap_urls),
            "coverage_percentage": (len(crawled_urls & sitemap_urls) / len(sitemap_urls) * 100 
                                 if sitemap_urls else 0)
        }

    async def crawl(self, start_url: str, output_dir: str):
        """Main crawl function"""
        os.makedirs(output_dir, exist_ok=True)
        self.base_domain = urlparse(start_url).netloc
        if self.base_domain.startswith('www.'):
            self.base_domain = self.base_domain[4:]

        async with AsyncWebCrawler(headless=True, verbose=True) as crawler:
            self.crawler = crawler
            
            # Initialize sitemap parser
            async with aiohttp.ClientSession() as session:
                self.sitemap = SitemapParser(session, self.base_path)
                await self.sitemap.fetch_sitemap(start_url)

            # Initialize progress bar
            self.progress_bar = tqdm(total=self.max_pages, desc="Crawling pages")

            # Initialize crawl queue with start URL
            queue = [(start_url, 0)]  # (url, depth)
            
            while queue and len(self.visited) < self.max_pages:
                current_url, depth = queue.pop(0)
                
                if current_url in self.queued:
                    continue
                    
                self.queued.add(current_url)
                new_links = await self.crawl_page(current_url, depth, output_dir)
                
                # Add new links to queue
                for link in new_links:
                    if (link not in self.queued and 
                        link not in self.visited and 
                        len(queue) < self.max_pages):
                        queue.append((link, depth + 1))

            # Close progress bar
            self.progress_bar.close()

            # Generate reports
            coverage = await self.validate_coverage()
            
            # Save reports
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_path = os.path.join(output_dir, f"crawl_report_{timestamp}.txt")
            
            # Save URL mappings to JSON
            import json
            url_mappings = {}
            for url in self.visited:
                filepath = self.url_to_filepath(url, "").lstrip("/")  # Get relative path
                url_mappings[filepath] = url
                
            mappings_path = os.path.join(output_dir, "url_mappings.json")
            with open(mappings_path, "w", encoding="utf-8") as f:
                json.dump(url_mappings, f, indent=2)
            
            logger.info(f"URL mappings saved to {mappings_path}")
            
            with open(report_path, "w") as f:
                f.write(self.stats.generate_report())
                f.write("\n\nCoverage Report:\n")
                f.write("================\n")
                for key, value in coverage.items():
                    f.write(f"{key}: {value}\n")
            
            logger.info(f"Crawl completed. Report saved to {report_path}")
            logger.info(f"Coverage: {coverage['coverage_percentage']:.2f}%")

async def main():
    """Example usage"""
    crawler = ImprovedWebCrawler(
        max_depth=5,
        max_pages=1000,
        delay=1.0,
        max_retries=3,
        base_path="/en/"  # Only crawl /en/ pages
    )
    
    await crawler.crawl(
        start_url="https://docs.snowflake.com/en/sql-reference/parameters",
        output_dir="docs_snowflake_crawl_missed"
    )

if __name__ == "__main__":
    asyncio.run(main())