import psutil
import random
import asyncio
from typing import List

class MemoryMonitor:
    """Monitor memory usage"""
    def __init__(self, threshold_mb: int = 1000):
        self.process = psutil.Process()
        self.threshold_mb = threshold_mb
    
    def check_memory(self) -> bool:
        """Check if memory usage is below threshold"""
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        return memory_mb < self.threshold_mb
    
    def get_memory_usage(self) -> float:
        """Get current memory usage in MB"""
        return self.process.memory_info().rss / 1024 / 1024

class AntiBot:
    """Anti-bot measures and utilities"""
    
    USER_AGENTS: List[str] = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]

    def __init__(self, requests_per_second: float = 2.0):
        self.requests_per_second = requests_per_second

    @classmethod
    def get_random_user_agent(cls) -> str:
        """Get a random user agent"""
        return random.choice(cls.USER_AGENTS)

    async def random_delay(self):
        """Implement random delay between requests"""
        delay = random.uniform(1.0, 3.0) / self.requests_per_second
        await asyncio.sleep(delay)
