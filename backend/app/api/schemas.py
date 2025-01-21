from pydantic import BaseModel, HttpUrl
from typing import List, Dict, Optional
from datetime import datetime

class CrawlerSettings(BaseModel):
    """Settings for crawler configuration"""
    browser_type: str = "chromium"
    performance_mode: str = "Medium"
    batch_size: int = 10
    max_depth: int = 3
    exclusion_patterns: List[str] = []
    save_images: bool = True
    capture_screenshots: bool = True
    generate_pdfs: bool = True
    
    class Config:
        schema_extra = {
            "example": {
                "browser_type": "chromium",
                "performance_mode": "High",
                "batch_size": 5,
                "max_depth": 2,
                "exclusion_patterns": [r".*\.pdf$", r"^/api/"],
                "save_images": True,
                "capture_screenshots": True,
                "generate_pdfs": False
            }
        }

class URLNode(BaseModel):
    """URL node in discovery graph"""
    id: str
    depth: int
    metadata: Optional[Dict] = None

class URLLink(BaseModel):
    """URL link in discovery graph"""
    source: str
    target: str
    type: Optional[str] = None

class URLGraph(BaseModel):
    """Graph representation of discovered URLs"""
    nodes: List[URLNode]
    links: List[URLLink]

class DiscoveryTask(BaseModel):
    """URL discovery task"""
    task_id: str
    start_url: str
    mode: str
    settings: CrawlerSettings
    status: str
    progress: float
    discovered_urls: List[str] = []
    total_urls: Optional[int] = None
    max_depth: Optional[int] = None
    url_graph: Optional[URLGraph] = None
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime
    current_url: Optional[str] = None

class TaskMetrics(BaseModel):
    """Task metrics information"""
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    urls_per_second: float = 0.0
    memory_usage: int = 0
    batch_processing_time: float = 0.0
    total_processing_time: float = 0.0
    saved_content: Dict[str, int] = {
        "markdown": 0,
        "images": 0,
        "pdf": 0,
        "screenshot": 0
    }

class CrawlTask(BaseModel):
    """Crawling task"""
    task_id: str
    urls: List[str]
    settings: CrawlerSettings
    status: str
    progress: float
    metrics: TaskMetrics
    error: Optional[str] = None
    created_at: datetime
    updated_at: datetime

class CrawlResult(BaseModel):
    """Crawling result"""
    url: str
    success: bool
    html: Optional[str]
    cleaned_html: Optional[str]
    error_message: Optional[str]
    media_data: Dict
    links_data: Dict[str, List[str]]
    created_at: datetime

class FileInfo(BaseModel):
    """File information"""
    url: str
    file_type: str
    stage_path: str
    content_type: Optional[str]
    size: int
    metadata: Optional[Dict]
    created_at: datetime

class WebSocketMessage(BaseModel):
    """Base WebSocket message"""
    task_id: str
    message_type: str
    data: Dict

class MetricsMessage(WebSocketMessage):
    """Metrics update message"""
    message_type: str = "metrics"
    data: TaskMetrics

class ProgressMessage(WebSocketMessage):
    """Progress update message"""
    message_type: str = "progress"
    data: Dict[str, float]  # progress, status

class ErrorMessage(WebSocketMessage):
    """Error message"""
    message_type: str = "error"
    data: Dict[str, str]  # error message