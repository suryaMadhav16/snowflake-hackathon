"""
Core crawler package initialization.

This package provides the core web crawling functionality including:
- BatchCrawler: Main crawler implementation for batch processing
- URLManager: URL handling and management
"""

from .crawler import BatchCrawler
from .url_manager import URLManager

__all__ = ['BatchCrawler', 'URLManager']
