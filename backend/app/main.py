from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import logging
import sys
import os
import uvicorn
from contextlib import asynccontextmanager

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.config import settings
from database.snowflake_manager import SnowflakeManager
from api.routes import router as api_router

logger = logging.getLogger(__name__)

app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.API_VERSION,
    docs_url=f"/api/{settings.API_VERSION}/docs",
    redoc_url=f"/api/{settings.API_VERSION}/redoc"
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.BACKEND_CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Add Gzip compression
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Include API router
app.include_router(
    api_router,
    prefix=f"/api/{settings.API_VERSION}"
)

@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "name": settings.PROJECT_NAME,
        "version": settings.API_VERSION,
        "docs": f"/api/{settings.API_VERSION}/docs"
    }

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifecycle manager for the FastAPI application"""
    try:
        # Startup logic
        snowflake = SnowflakeManager()
        # await snowflake.initialize_environment()
        logger.info("Snowflake environment initialized successfully")
        yield
        # Shutdown logic
        snowflake.close()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Application lifecycle error: {str(e)}")
        yield
if __name__ == "__main__":
    uvicorn.run(
        "main:app",  # Use import string format
        host="localhost", 
        port=8000, 
        reload=True
    )