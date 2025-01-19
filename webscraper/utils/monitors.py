import psutil
import random
import asyncio
from typing import List
import logging

logger = logging.getLogger(__name__)

class MemoryMonitor:
    """Monitor memory usage for Crawl4AI operations"""
    def __init__(self, threshold_mb: int = 1000):
        self.process = psutil.Process()
        self.threshold_mb = threshold_mb
        self.warning_threshold_mb = threshold_mb * 0.8  # 80% of max
        self.last_check = 0
        
    def check_memory(self) -> bool:
        """Check if memory usage is below threshold
        
        Returns:
            bool: True if memory usage is acceptable, False if above threshold
        """
        current_usage = self.get_memory_usage()
        
        # Warning level
        if current_usage > self.warning_threshold_mb:
            logger.warning(f"High memory usage: {current_usage:.1f}MB")
            
        # Critical level
        if current_usage > self.threshold_mb:
            logger.error(f"Memory threshold exceeded: {current_usage:.1f}MB")
            return False
            
        return True
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB
        
        Returns:
            float: Current memory usage in megabytes
        """
        try:
            return self.process.memory_info().rss / 1024 / 1024
        except Exception as e:
            logger.error(f"Error getting memory usage: {str(e)}")
            return 0.0
            
    def get_memory_stats(self) -> dict:
        """Get detailed memory statistics
        
        Returns:
            dict: Memory usage statistics
        """
        try:
            memory = self.process.memory_full_info()
            return {
                'rss': memory.rss / 1024 / 1024,
                'vms': memory.vms / 1024 / 1024,
                'shared': memory.shared / 1024 / 1024 if hasattr(memory, 'shared') else 0,
                'percent': self.process.memory_percent()
            }
        except Exception as e:
            logger.error(f"Error getting memory stats: {str(e)}")
            return {}

class AntiBot:
    """Anti-bot measures optimized for Crawl4AI
    
    Note: Most anti-bot features are now handled by Crawl4AI's built-in
    capabilities. This class provides additional customization options.
    """
    
    USER_AGENTS: List[str] = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]
    
    # Common bot detection triggers to avoid
    RISKY_PATTERNS = [
        'selenium', 'webdriver', 'puppeteer', 'automation', 'bot',
        'crawler', 'spider', 'headless'
    ]

    def __init__(self, requests_per_second: float = 2.0):
        self.requests_per_second = requests_per_second
        self.last_request_time = 0
        self.total_requests = 0
        self.failed_requests = 0

    @classmethod
    def get_random_user_agent(cls) -> str:
        """Get a random user agent string
        
        Returns:
            str: Random user agent string
        """
        return random.choice(cls.USER_AGENTS)

    async def random_delay(self):
        """Implement random delay between requests with jitter
        
        This supplements Crawl4AI's built-in delays with additional
        randomization to appear more human-like.
        """
        base_delay = 1.0 / self.requests_per_second
        jitter = random.uniform(-0.1 * base_delay, 0.3 * base_delay)
        delay = base_delay + jitter
        await asyncio.sleep(delay)
        
    def track_request(self, success: bool = True):
        """Track request success/failure rates
        
        Args:
            success (bool): Whether the request succeeded
        """
        self.total_requests += 1
        if not success:
            self.failed_requests += 1
            
    def get_stats(self) -> dict:
        """Get anti-bot statistics
        
        Returns:
            dict: Statistics about requests and failures
        """
        return {
            'total_requests': self.total_requests,
            'failed_requests': self.failed_requests,
            'failure_rate': (self.failed_requests / self.total_requests) if self.total_requests > 0 else 0
        }