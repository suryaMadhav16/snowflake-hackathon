"""Utility functions for the crawler"""

import os
import re
import logging
from pathlib import Path
from typing import Set, List, Optional
from urllib.parse import urlparse, urljoin

logger = logging.getLogger(__name__)

def clean_url(url: str) -> str:
    """Clean and normalize URL"""
    # Remove fragments
    url = re.sub(r'#.*$', '', url)
    # Remove trailing slashes
    url = url.rstrip('/')
    return url

def is_valid_url(url: str) -> bool:
    """Check if URL is valid"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def is_same_domain(url1: str, url2: str) -> bool:
    """Check if two URLs belong to the same domain"""
    try:
        domain1 = urlparse(url1).netloc
        domain2 = urlparse(url2).netloc
        return domain1 == domain2
    except Exception:
        return False

def extract_filename_from_url(url: str) -> str:
    """Extract a valid filename from URL"""
    # Remove scheme and domain
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    
    # Replace invalid characters
    filename = re.sub(r'[^\w\-_.]', '_', path)
    
    # Handle empty or invalid cases
    if not filename:
        filename = 'index'
    
    return filename

def setup_logging(log_dir: Optional[Path] = None, debug: bool = False):
    """Setup logging configuration"""
    log_level = logging.DEBUG if debug else logging.INFO
    
    # Basic configuration
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Add file handler if log_dir is specified
    if log_dir:
        log_dir.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_dir / 'crawler.log')
        file_handler.setLevel(log_level)
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        
        # Add file handler to root logger
        logging.getLogger('').addHandler(file_handler)

def sanitize_filename(filename: str) -> str:
    """Sanitize filename to be safe across platforms"""
    # Replace invalid characters
    filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
    
    # Ensure filename isn't too long (max 255 chars)
    if len(filename) > 255:
        base, ext = os.path.splitext(filename)
        filename = f"{base[:250]}{ext}"
        
    return filename

def create_directory_structure(base_dir: Path, website: str) -> dict:
    """Create necessary directory structure for website crawling"""
    dirs = {
        'raw': base_dir / 'raw' / website,
        'processed': base_dir / 'processed' / website,
        'images': base_dir / 'images' / website,
        'logs': base_dir / 'logs' / website,
    }
    
    for dir_path in dirs.values():
        dir_path.mkdir(parents=True, exist_ok=True)
        
    return dirs

def extract_code_blocks(markdown: str) -> List[dict]:
    """Extract code blocks from markdown content"""
    code_blocks = []
    pattern = r'```(\w+)?\n(.*?)```'
    
    for match in re.finditer(pattern, markdown, re.DOTALL):
        language = match.group(1) or 'text'
        code = match.group(2).strip()
        
        code_blocks.append({
            'language': language,
            'code': code
        })
        
    return code_blocks

def normalize_image_url(base_url: str, image_url: str) -> str:
    """Normalize image URL to absolute URL"""
    if not bool(urlparse(image_url).netloc):
        return urljoin(base_url, image_url)
    return image_url

def extract_image_urls(markdown: str) -> Set[str]:
    """Extract image URLs from markdown content"""
    # Match both markdown and HTML image patterns
    markdown_pattern = r'!\[.*?\]\((.*?)\)'
    html_pattern = r'<img[^>]*src=[\'"]([^\'"]*)[\'"]'
    
    urls = set()
    
    # Find markdown images
    urls.update(re.findall(markdown_pattern, markdown))
    
    # Find HTML images
    urls.update(re.findall(html_pattern, markdown))
    
    return urls

def generate_image_filename(url: str, index: int) -> str:
    """Generate a unique filename for an image"""
    # Extract original extension if present
    orig_ext = os.path.splitext(urlparse(url).path)[1]
    ext = orig_ext if orig_ext else '.png'
    
    # Create base filename
    base = f"image_{index}"
    
    # Ensure extension starts with dot
    if not ext.startswith('.'):
        ext = f".{ext}"
        
    return sanitize_filename(f"{base}{ext}")

class RateLimiter:
    """Simple rate limiter for crawling"""
    
    def __init__(self, requests_per_second: float = 2.0):
        self.delay = 1.0 / requests_per_second
        self.last_request_time = 0.0
        
    async def wait(self):
        """Wait if necessary to maintain rate limit"""
        import time
        import asyncio
        
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        if time_since_last < self.delay:
            await asyncio.sleep(self.delay - time_since_last)
            
        self.last_request_time = time.time()
