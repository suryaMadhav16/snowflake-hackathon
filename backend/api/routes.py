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

router = APIRouter()

# Load Snowflake configuration
snowflake_config = load_snowflake_config()
if error := validate_config(snowflake_config):
    raise Exception(f"Invalid Snowflake configuration: {error}")

# Default configurations
DEFAULT_BROWSER_CONFIG = BrowserConfig(
    headless=True,
    browser_type="chromium",
    user_agent_mode="random",
    viewport_width=1080,
    viewport_height=800
)

DEFAULT_CRAWLER_CONFIG = CrawlerRunConfig(
    magic=True,
    simulate_user=True,
    cache_mode=CacheMode.ENABLED,
    mean_delay=1.0,
    max_range=0.3,
    semaphore_count=5,
    screenshot=False,
    pdf=False,
    exclude_external_images=False,
    wait_for_images=True
)

@router.post("/discover", response_model=DiscoverURLResponse)
async def discover_urls(request: DiscoverURLRequest) -> DiscoverURLResponse:
    """Discover URLs from a target website"""
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
    """Crawl selected URLs"""
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
                        saved_files[file['file_type']] = file['file_path']
                
                results.append(CrawlResult(
                    url=result.url,
                    success=result.success,
                    files=saved_files,
                    error_message=result.error_message if not result.success else None
                ))
                
        return CrawlResponse(results=results)
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))