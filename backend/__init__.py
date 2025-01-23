"""
Backend package initialization.

This package contains the core functionality for the web crawler and data processing system.
It includes the following subpackages:
- api: REST API endpoints and models
- config: Configuration management
- src.core: Core crawler functionality
- src.database: Database interaction layer
- src.utils: Utility functions and helpers
"""

from .src.core import crawler
from .src.database import db_manager, snowflake_manager
from .src.utils import content_processor

__all__ = [
    'crawler',
    'db_manager',
    'snowflake_manager',
    'content_processor'
]
