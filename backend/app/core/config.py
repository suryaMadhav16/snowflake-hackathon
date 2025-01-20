from pydantic_settings import BaseSettings
from typing import Optional
from functools import lru_cache

class Settings(BaseSettings):
    """Application settings"""
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT: str
    SNOWFLAKE_USER: str
    SNOWFLAKE_PASSWORD: str
    SNOWFLAKE_DATABASE: str = "LLM"
    SNOWFLAKE_SCHEMA: str = "RAG"
    SNOWFLAKE_WAREHOUSE: str = "COMPUTE_WH"
    SNOWFLAKE_ROLE: str = "ACCOUNTADMIN"
    
    # API Configuration
    API_VERSION: str = "v1"
    PROJECT_NAME: str = "Web Crawler API"
    BACKEND_CORS_ORIGINS: list = ["*"]
    
    # Crawler Configuration
    BATCH_SIZE: int = 10
    MAX_CONCURRENT_TASKS: int = 5
    DEFAULT_TIMEOUT: int = 60000
    
    class Config:
        case_sensitive = True
        env_file = ".env"

@lru_cache()
def get_settings() -> Settings:
    """Get cached settings"""
    return Settings()

settings = get_settings()