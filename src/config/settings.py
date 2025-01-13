"""Configuration settings for the crawler"""

import os
from pathlib import Path
from typing import Optional
from pydantic import BaseModel, Field

class CrawlerSettings(BaseModel):
    """Crawler configuration settings"""

    # API Keys
    groq_api_key: str = Field(
        default_factory=lambda: os.getenv('GROQ_API_KEY', ''),
        description="Groq API key for LLM processing"
    )

    # Crawling Settings
    max_concurrent: int = Field(
        default=5,
        ge=1,
        le=20,
        description="Maximum number of concurrent crawls"
    )

    requests_per_second: float = Field(
        default=2.0,
        ge=0.1,
        description="Maximum requests per second"
    )

    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for failed requests"
    )

    # Browser Settings
    browser_args: list[str] = Field(
        default_factory=lambda: [
            "--disable-gpu",
            "--disable-dev-shm-usage",
            "--no-sandbox"
        ],
        description="Additional browser arguments"
    )

    # Processing Settings
    chunk_size: int = Field(
        default=5000,
        ge=1000,
        description="Size of text chunks for processing"
    )

    # Directory Settings
    base_dir: Path = Field(
        default=Path(__file__).parent.parent.parent / 'data',
        description="Base directory for all output"
    )

    # Logging Settings
    debug: bool = Field(
        default=True,
        description="Enable debug logging"
    )

    @classmethod
    def from_env(cls, env_file: Optional[Path] = None):
        """Load settings from environment variables"""
        if env_file and env_file.exists():
            from dotenv import load_dotenv
            load_dotenv(env_file)

        return cls()

    def validate_directories(self):
        """Ensure all required directories exist"""
        dirs = [
            self.base_dir,
            self.base_dir / 'raw',
            self.base_dir / 'processed',
            self.base_dir / 'images',
            self.base_dir / 'logs'
        ]

        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)

    class Config:
        env_prefix = 'CRAWLER_'
