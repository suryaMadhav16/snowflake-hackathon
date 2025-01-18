import asyncio
from threading import Lock
from typing import List, Set
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

class DomainCrawler:
    def __init__(self, max_concurrent: int = 5):
        self.visited_urls: Set[str] = set()
        self.url_lock = Lock()
        
        self.browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,
            user_agent_mode="random"
        )
        
        self.base_config = CrawlerRunConfig(
            magic=True,
            simulate_user=True,
            cache_mode=CacheMode.ENABLED,
            mean_delay=0.5,
            max_range=0.3,
            semaphore_count=max_concurrent
        )
        
        self.max_concurrent = max_concurrent

    async def _process_url(self, crawler: AsyncWebCrawler, url: str) -> dict:
        with self.url_lock:
            if url in self.visited_urls:
                return None
            self.visited_urls.add(url)
        
        try:
            result = await crawler.arun(
                url=url,
                config=self.base_config
            )
            
            if result.success:
                return {
                    'url': url,
                    'content': result.markdown_v2,
                    'links': result.links,
                    'media': result.media
                }
        except Exception as e:
            print(f"Error processing {url}: {str(e)}")
        return None

    async def crawl_domain(self, start_url: str, max_pages: int = 100) -> List[dict]:
        results = []
        urls_to_crawl = {start_url}
        
        async with AsyncWebCrawler(config=self.browser_config) as crawler:
            while urls_to_crawl and len(self.visited_urls) < max_pages:
                batch = list(urls_to_crawl)[:self.max_concurrent]
                urls_to_crawl = urls_to_crawl - set(batch)
                
                tasks = [
                    self._process_url(crawler, url) 
                    for url in batch
                ]
                
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in batch_results:
                    if result and isinstance(result, dict):
                        results.append(result)
                        with self.url_lock:
                            new_urls = set(result.get('links', []))
                            urls_to_crawl.update(
                                new_urls - self.visited_urls
                            )
        
        return results

async def main():
    # Initialize the crawler with desired concurrency
    crawler = DomainCrawler(max_concurrent=5)
    
    # Define target URL and max pages
    target_url = "https://crawl4ai.com/"
    max_pages = 50
    
    try:
        # Start crawling
        print(f"Starting crawl of {target_url}")
        results = await crawler.crawl_domain(
            start_url=target_url,
            max_pages=max_pages
        )
        
        # Process results
        print(f"\nCrawl completed. Found {len(results)} pages")
        for result in results:
            print(f"\nURL: {result['url']}")
            print(f"Links found: {len(result.get('links', []))}")
            print(f"Media items: {len(result.get('media', []))}")
            print(f"Content length: {len(result['content'])} characters")
            
    except Exception as e:
        print(f"Crawl failed with error: {str(e)}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
