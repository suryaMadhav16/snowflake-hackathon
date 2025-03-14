"""
API package initialization.

This package contains the REST API implementation including:
- routes: API endpoint definitions
- models: Data models and schemas
"""

from api.routes import *
from api.models import *

__all__ = ['routes', 'models']
