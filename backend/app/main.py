from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
import logging

from .core.config import settings
from .database.snowflake_manager import SnowflakeManager
from .api.routes import router as api_router

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

# Startup event
@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    try:
        # Initialize Snowflake environment
        snowflake = SnowflakeManager()
        # await snowflake.initialize_environment()
        logger.info("Snowflake environment initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize application: {str(e)}")
        # We allow the application to start even if initialization fails
        # This way we can fix issues while the app is running

# Shutdown event
@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    try:
        # Close any active Snowflake connections
        snowflake = SnowflakeManager()
        snowflake.close()
        logger.info("Application shutdown complete")
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")
