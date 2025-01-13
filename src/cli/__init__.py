"""CLI package for documentation crawler

This package contains the command-line interface for the documentation crawler.
The main entry point is the `main.py` module which handles argument parsing
and crawler initialization.
"""

from .main import main

__version__ = '0.1.0'
__all__ = ['main']
