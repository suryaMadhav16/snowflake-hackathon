import json
import os
import asyncio
from typing import Set, Dict, List
from datetime import datetime
import logging
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from tqdm import tqdm
from dataclasses import dataclass, field
from collections import defaultdict

# Import the previous crawler
from cr import ImprovedWebCrawler, CrawlStats
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class ConsolidatedStats:
    """Track statistics across multiple crawls"""
    initial_crawl: Dict = field(default_factory=dict)
    followup_crawl: Dict = field(default_factory=dict)
    total_pages: int = 0
    total_missed: int = 0
    uncrawlable_urls: List[str] = field(default_factory=list)
    
    def generate_report(self) -> str:
        """Generate a comprehensive report of all crawls"""
        report = [
            "Consolidated Crawl Report",
            "========================",
            f"Total Unique Pages Crawled: {self.total_pages}",
            f"Initial Crawl Pages: {len(self.initial_crawl)}",
            f"Follow-up Crawl Pages: {len(self.followup_crawl)}",
            f"Remaining Uncrawlable URLs: {len(self.uncrawlable_urls)}",
            "\nInitial Crawl URLs:",
            *[f"  {url}" for url in sorted(self.initial_crawl.keys())],
            "\nFollow-up Crawl URLs:",
            *[f"  {url}" for url in sorted(self.followup_crawl.keys())],
            "\nUncrawlable URLs:",
            *[f"  {url}" for url in sorted(self.uncrawlable_urls)]
        ]
        return "\n".join(report)

class FollowUpCrawler(ImprovedWebCrawler):
    """Crawler specifically for handling missed URLs from initial crawl"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.consolidated_stats = ConsolidatedStats()
        
    async def load_initial_crawl_data(self, output_dir: str) -> Set[str]:
        """Load data from the initial crawl"""
        # Load URL mappings from initial crawl
        mappings_path = os.path.join(output_dir, "url_mappings.json")
        try:
            with open(mappings_path, 'r', encoding='utf-8') as f:
                mappings = json.load(f)
                self.consolidated_stats.initial_crawl = mappings
                return set(mappings.values())
        except FileNotFoundError:
            logger.error(f"Could not find initial crawl mappings at {mappings_path}")
            return set()

    def load_missed_urls(self, report_path: str) -> List[str]:
        """Extract missed URLs from the crawl report"""
        missed_urls = []
        try:
            with open(report_path, 'r', encoding='utf-8') as f:
                content = f.read()
                # Find the missed_urls section in the coverage report
                if 'missed_urls' in content:
                    # Extract URLs from the list format
                    import ast
                    start_idx = content.find('missed_urls') + content[content.find('missed_urls'):].find('[')
                    end_idx = content.find(']', start_idx) + 1
                    urls_str = content[start_idx:end_idx]
                    missed_urls = ast.literal_eval(urls_str)
        except Exception as e:
            logger.error(f"Error reading missed URLs from report: {str(e)}")
        return missed_urls

    async def crawl_missed_urls(self, missed_urls: List[str], output_dir: str):
        """Crawl the missed URLs and save their content"""
        if not missed_urls:
            logger.info("No missed URLs to crawl")
            return

        # Initialize progress bar for missed URLs
        self.progress_bar = tqdm(total=len(missed_urls), desc="Crawling missed URLs")

        # Create a subdirectory for follow-up crawl
        followup_dir = os.path.join(output_dir, "followup_crawl")
        os.makedirs(followup_dir, exist_ok=True)

        async with AsyncWebCrawler(headless=True, verbose=True) as crawler:
            self.crawler = crawler
            
            for url in missed_urls:
                if not self.is_valid_url(url):
                    self.consolidated_stats.uncrawlable_urls.append(url)
                    continue

                new_links = await self.crawl_page(url, 0, followup_dir)
                if new_links is not None:  # Successfully crawled
                    filepath = self.url_to_filepath(url, "").lstrip("/")
                    self.consolidated_stats.followup_crawl[filepath] = url

        self.progress_bar.close()

    def save_consolidated_reports(self, output_dir: str):
        """Save all final reports"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Save consolidated stats report
        report_path = os.path.join(output_dir, f"consolidated_report_{timestamp}.txt")
        with open(report_path, 'w', encoding='utf-8') as f:
            f.write(self.consolidated_stats.generate_report())
        
        # Save updated URL mappings
        all_mappings = {
            "initial_crawl": self.consolidated_stats.initial_crawl,
            "followup_crawl": self.consolidated_stats.followup_crawl,
            "uncrawlable_urls": self.consolidated_stats.uncrawlable_urls
        }
        
        mappings_path = os.path.join(output_dir, f"final_url_mappings_{timestamp}.json")
        with open(mappings_path, 'w', encoding='utf-8') as f:
            json.dump(all_mappings, f, indent=2)
        
        logger.info(f"Consolidated report saved to {report_path}")
        logger.info(f"Final URL mappings saved to {mappings_path}")

async def main():
    """Example usage of the follow-up crawler"""
    # Initial setup
    base_output_dir = "docs_snowflake_crawl"
    initial_report = "consolidated_report_20250114_021629.txt"  # Replace with actual report path
    
    # Create follow-up crawler
    crawler = FollowUpCrawler(
        max_depth=5,
        max_pages=1000,
        delay=1.0,
        max_retries=3,
        base_path="/en/"
    )
    
    # Load initial crawl data
    initial_urls = await crawler.load_initial_crawl_data(base_output_dir)
    
    # Load and crawl missed URLs
    missed_urls = crawler.load_missed_urls(initial_report)
    await crawler.crawl_missed_urls(missed_urls, base_output_dir)
    
    # Generate and save final reports
    crawler.save_consolidated_reports(base_output_dir)

if __name__ == "__main__":
    asyncio.run(main())