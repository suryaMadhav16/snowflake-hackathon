from typing import List, Dict, Optional
from pydantic import BaseModel, HttpUrl

class DiscoverURLRequest(BaseModel):
    """Request model for URL discovery endpoints.

    This model validates and structures the input for URL discovery operations,
    supporting both single URL validation and full crawl modes.

    Attributes:
        url (str): The target URL to start discovery from
        mode (str): The discovery mode to use. Must be either:
            - "single": Validates and processes a single URL
            - "full": Performs full crawling with sitemap processing

    Example:
        >>> request = DiscoverURLRequest(
        ...     url="https://example.com",
        ...     mode="full"
        ... )
    """
    url: str
    mode: str  # "single" or "full"

class DiscoverURLResponse(BaseModel):
    """Response model for URL discovery operations.

    Contains the list of discovered URLs and the associated domain information.

    Attributes:
        urls (List[str]): List of discovered URLs
        domain (str): The domain these URLs belong to

    Example:
        >>> response = DiscoverURLResponse(
        ...     urls=["https://example.com/page1", "https://example.com/page2"],
        ...     domain="example.com"
        ... )
        >>> print(f"Found {len(response.urls)} URLs for {response.domain}")
    """
    urls: List[str]
    domain: str

class CrawlRequest(BaseModel):
    """Request model for initiating crawl operations.

    Defines the URLs to crawl and optional patterns to exclude from crawling.

    Attributes:
        urls (List[str]): List of URLs to crawl
        exclude_patterns (Optional[List[str]]): Regular expression patterns for URLs to exclude.
            Defaults to None.

    Example:
        >>> request = CrawlRequest(
        ...     urls=["https://example.com"],
        ...     exclude_patterns=["^/admin/", "^/private/"]
        ... )
    """
    urls: List[str]
    exclude_patterns: Optional[List[str]] = None

class CrawlResult(BaseModel):
    """Model representing the result of crawling a single URL.

    Contains information about the crawl operation success/failure and
    paths to any files generated during crawling.

    Attributes:
        url (str): The URL that was crawled
        success (bool): Whether the crawl operation was successful
        files (Dict[str, str]): Mapping of content types to file paths
            Example: {'markdown': '/path/to/content.md', 'pdf': '/path/to/doc.pdf'}
        error_message (Optional[str]): Error message if crawl failed, None if successful

    Example:
        >>> result = CrawlResult(
        ...     url="https://example.com/page",
        ...     success=True,
        ...     files={"markdown": "/tmp/content.md"},
        ...     error_message=None
        ... )
    """
    url: str
    success: bool
    files: Dict[str, str]  # type -> file_path
    error_message: Optional[str] = None

class CrawlResponse(BaseModel):
    """Response model containing results from a batch crawl operation.

    Aggregates the results of crawling multiple URLs into a single response.

    Attributes:
        results (List[CrawlResult]): List of individual URL crawl results

    Example:
        >>> response = CrawlResponse(
        ...     results=[
        ...         CrawlResult(url="https://example.com/1", success=True),
        ...         CrawlResult(url="https://example.com/2", success=False)
        ...     ]
        ... )
        >>> successful = sum(1 for r in response.results if r.success)
        >>> print(f"Successfully crawled {successful} URLs")
    """
    results: List[CrawlResult]
