from typing import List, Dict, Optional
from pydantic import BaseModel, HttpUrl

class DiscoverURLRequest(BaseModel):
    url: str
    mode: str  # "single" or "full"

class DiscoverURLResponse(BaseModel):
    urls: List[str]
    domain: str

class CrawlRequest(BaseModel):
    urls: List[str]
    exclude_patterns: Optional[List[str]] = None

class CrawlResult(BaseModel):
    url: str
    success: bool
    files: Dict[str, str]  # type -> file_path
    error_message: Optional[str] = None

class CrawlResponse(BaseModel):
    results: List[CrawlResult]