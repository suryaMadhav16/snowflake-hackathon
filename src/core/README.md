# Core Module Documentation

This module contains the core functionality for web crawling and URL management operations.

## Overview

The core module consists of two main components:
1. BatchCrawler - Handles efficient batch crawling operations
2. URLManager - Manages URL discovery and tracking

## Files Structure

```
core/
├── __init__.py    - Exports BatchCrawler and URLManager
├── crawler.py     - Contains BatchCrawler implementation
└── url_manager.py - Contains URLManager implementation
```

## Components Documentation

### BatchCrawler (crawler.py)

Main class for handling batched crawling operations with resource management.

#### Class Signature
```python
class BatchCrawler:
    def __init__(
        self, 
        browser_config: BrowserConfig = None,
        crawl_config: CrawlerRunConfig = None
    )
```

#### Methods

##### `process_batch`
```python
async def process_batch(
    self, 
    urls: List[str], 
    batch_size: int = 10
) -> AsyncGenerator[List[CrawlResult], None]
```
- Processes URLs in configurable batch sizes for efficient crawling
- Yields CrawlResult objects for each processed batch

##### `get_metrics`
```python
def get_metrics(self) -> Dict
```
- Returns current crawling metrics including success/failure counts and timing information
- Provides real-time monitoring capabilities

### URLManager (url_manager.py)

Manages URL discovery and tracking for the web crawler.

#### Class Signature
```python
class URLManager:
    def __init__(self, discovery_config: Optional[CrawlerRunConfig] = None)
```

#### Methods

##### `discover_urls`
```python
async def discover_urls(self, base_url: str, max_depth: int = 3) -> List[str]
```
- Discovers URLs starting from a base URL up to a specified depth
- Handles same-domain filtering and URL normalization

##### `get_unprocessed_urls`
```python
def get_unprocessed_urls(self) -> List[str]
```
- Returns list of discovered URLs that haven't been processed
- Used for tracking crawling progress

##### `mark_as_processed`
```python
def mark_as_processed(self, urls: List[str])
```
- Marks URLs as processed in the tracking system
- Maintains crawling state

##### `get_url_stats`
```python
def get_url_stats(self) -> Dict
```
- Returns statistics about discovered and processed URLs
- Provides metrics for monitoring crawl coverage

## Private Helper Methods

### URLManager

##### `_is_same_domain`
```python
def _is_same_domain(self, url: str) -> bool
```
- Checks if URL belongs to the same domain as base URL
- Handles subdomain validation

##### `_normalize_url`
```python
def _normalize_url(self, url: str) -> Optional[str]
```
- Normalizes URL format for consistency
- Handles various URL formats and edge cases

##### `_extract_urls_from_result`
```python
def _extract_urls_from_result(self, result, base_url: str) -> Set[str]
```
- Extracts and normalizes URLs from crawl results
- Processes both internal and external links

## Key Features

1. **Efficient Batch Processing**
   - Configurable batch sizes
   - Resource-aware crawling
   - Real-time metrics tracking

2. **URL Management**
   - URL discovery with depth control
   - Domain filtering
   - URL normalization
   - Progress tracking

3. **Error Handling**
   - Comprehensive error logging
   - Failure recovery
   - Batch-level error isolation

4. **Performance Monitoring**
   - Detailed metrics collection
   - Processing speed tracking
   - Success/failure statistics

## Usage Example

```python
from core import BatchCrawler, URLManager

# Initialize URL manager
url_manager = URLManager()

# Discover URLs
urls = await url_manager.discover_urls("https://example.com", max_depth=2)

# Initialize batch crawler
crawler = BatchCrawler()

# Process URLs in batches
async for results in crawler.process_batch(urls, batch_size=5):
    # Handle results
    url_manager.mark_as_processed([r.url for r in results if r.success])
    
    # Get metrics
    metrics = crawler.get_metrics()
    stats = url_manager.get_url_stats()
```

## Dependencies

- crawl4ai - Web crawling framework
- asyncio - Asynchronous I/O
- urllib.parse - URL parsing and manipulation
- logging - Error and debug logging
