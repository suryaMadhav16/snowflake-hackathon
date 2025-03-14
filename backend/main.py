"""FastAPI backend server for the Web Crawler application.

This module sets up and configures the FastAPI server that provides the web crawling API.
It handles:
1. Python path configuration for imports
2. FastAPI application setup with CORS
3. API route registration
4. Health check endpoint
5. Server startup configuration

The server provides RESTful endpoints for URL discovery and web crawling operations,
with full OpenAPI/Swagger documentation support.

Example:
    To run the server:
        $ python backend/main.py

    This will start the server on http://0.0.0.0:8000 with auto-reload enabled.
    API documentation will be available at:
    - Swagger UI: http://localhost:8000/docs
    - ReDoc: http://localhost:8000/redoc
"""

import os
import sys

# Add src directory to Python path
src_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'src')
sys.path.insert(0, src_dir)

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import logging

from api.routes import router

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Web Crawler API",
    description="""
    API for discovering and crawling web pages. Provides endpoints for:
    - URL discovery with sitemap support
    - Configurable web crawling
    - Content extraction and storage
    """,
    version="1.0.0"
)

# Enable CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

# Include our router
app.include_router(router, prefix="/api/v1")

@app.get("/health")
async def health_check():
    """Health check endpoint for monitoring service status.

    This endpoint provides a simple way to verify that the API server
    is running and responsive.

    Returns:
        dict: Status information containing:
            - status (str): "healthy" if the service is running properly

    Example:
        >>> response = await client.get("/health")
        >>> assert response.json() == {"status": "healthy"}
    """
    return {"status": "healthy"}

if __name__ == "__main__":
    logger.info("Starting Web Crawler API server...")
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True  # Enable auto-reload for development
    )
