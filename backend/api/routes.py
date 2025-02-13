from fastapi import APIRouter, HTTPException
from typing import List, Dict
from urllib.parse import urlparse
import asyncio

import os
import sys

# Add the backend directory to Python path
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
src_dir = os.path.join(backend_dir, 'src')
sys.path.insert(0, src_dir)

from core.url_manager import URLManager
from core.crawler import BatchCrawler
from database.db_manager import DatabaseManager
from config.snowflake import load_snowflake_config, validate_config
from .models import (
    DiscoverURLRequest, 
    DiscoverURLResponse,
    CrawlRequest,
    CrawlResponse,
    CrawlResult
)

from crawl4ai import BrowserConfig, CrawlerRunConfig, CacheMode
from crawl4ai.content_filter_strategy import PruningContentFilter
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator


router = APIRouter()

# Load Snowflake configuration
snowflake_config = load_snowflake_config()
if error := validate_config(snowflake_config):
    raise Exception(f"Invalid Snowflake configuration: {error}")

# Browser configuration for headless web crawling
DEFAULT_BROWSER_CONFIG = BrowserConfig(
    headless=True,
    browser_type="chromium",
    user_agent_mode="random",
    viewport_width=1080,
    viewport_height=800
)
# TODO: Do further research on the PruningContentFilter and DefaultMarkdownGenerator classes. They not working as expected. So sticking to the default configuration for now.
prune_filter = PruningContentFilter(
    user_query="only filter nav bars",    
    threshold=0.5,           
    threshold_type="dynamic",      
    min_word_threshold=0
)

md_generator = DefaultMarkdownGenerator(content_filter=prune_filter)


# Default crawler behavior configuration
DEFAULT_CRAWLER_CONFIG = CrawlerRunConfig(
    magic=True,              # Enable intelligent content extraction
    simulate_user=True,      # Simulate real user behavior
    cache_mode=CacheMode.ENABLED,  # Enable response caching
    mean_delay=1.0,         # Average delay between requests
    max_range=0.3,          # Maximum random delay variation
    semaphore_count=5,      # Maximum concurrent requests
    screenshot=False,        # Disable screenshot capture
    pdf=False,              # Disable PDF generation
    exclude_external_images=False,  # Include external images
    wait_for_images=False,    # Wait for image loading,
    markdown_generator=md_generator,
    excluded_tags=["form", "header", "footer", "nav"],

)

@router.post("/discover", response_model=DiscoverURLResponse)
async def discover_urls(request: DiscoverURLRequest) -> DiscoverURLResponse:
    """Discover URLs from a target website.
    
    This endpoint supports two modes of URL discovery:
    1. Single mode: Validates and processes a single URL
    2. Full mode: Performs complete crawling with sitemap processing

    Args:
        request (DiscoverURLRequest): Contains:
            - url: Target website URL
            - mode: "single" or "full"

    Returns:
        DiscoverURLResponse: Contains discovered URLs and domain info

    Raises:
        HTTPException: 
            - 404: If no URLs are discovered
            - 500: For any other processing errors

    Example:
        >>> response = await discover_urls(DiscoverURLRequest(
        ...     url="https://example.com",
        ...     mode="full"
        ... ))
        >>> print(f"Found {len(response.urls)} URLs")
    """
    try:
        url_manager = URLManager()
        
        if request.mode == "single":
            discovered_urls = await url_manager.discover_single_url(request.url)
        else:
            discovered_urls = await url_manager.discover_urls(request.url)
            
        if not discovered_urls:
            raise HTTPException(status_code=404, detail="No URLs discovered")
            
        domain = urlparse(request.url).netloc
        return DiscoverURLResponse(
            urls=discovered_urls,
            domain=domain
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/crawl", response_model=CrawlResponse)
async def crawl_urls(request: CrawlRequest) -> CrawlResponse:
    """Crawl a list of URLs and store their content.
    
    This endpoint processes each URL through a configurable crawler that:
    1. Fetches and processes webpage content
    2. Extracts and saves various content types
    3. Stores results in Snowflake
    4. Tracks success/failure for each URL

    Args:
        request (CrawlRequest): Contains:
            - urls: List of URLs to crawl
            - exclude_patterns: Optional regex patterns for URL exclusion

    Returns:
        CrawlResponse: Contains crawl results for each URL including:
            - Success/failure status
            - Paths to saved files
            - Error messages if applicable

    Raises:
        HTTPException: 500 status for processing errors

    Example:
        >>> response = await crawl_urls(CrawlRequest(
        ...     urls=["https://example.com/page1"],
        ...     exclude_patterns=["^/private/"]
        ... ))
        >>> for result in response.results:
        ...     print(f"{result.url}: {'Success' if result.success else 'Failed'}")
    """
    try:
        # Initialize DatabaseManager with Snowflake config
        db = DatabaseManager(config=snowflake_config)
        await db.initialize()
        
        crawler = BatchCrawler(
            browser_config=DEFAULT_BROWSER_CONFIG,
            crawl_config=DEFAULT_CRAWLER_CONFIG,
            excluded_patterns=request.exclude_patterns,
            db=db  # Pass configured db instance
        )
        
        results = []        
        async for batch_results in crawler.process_batch(request.urls):
            for result in batch_results:
                # Get saved files for this result from database
                saved_files = {}
                
                if result.success:
                    files = await db.get_saved_files(result.url)
                    for file in files:
                        print(f"Found saved file: {file['URL']}")
                        saved_files[file['FILE_NAME']] = file['URL']
                
                results.append(CrawlResult(
                    url=result.url,
                    success=result.success,
                    files=saved_files,
                    error_message=result.error_message if not result.success else None
                ))
                
        return CrawlResponse(results=results)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
