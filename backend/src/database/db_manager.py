import json
import logging
from typing import Dict, List, Set, Optional
from datetime import datetime
from pathlib import Path
from crawl4ai import CrawlResult
from .snowflake_manager import SnowflakeManager

logger = logging.getLogger(__name__)

class DatabaseManager(SnowflakeManager):
    """Manages database operations for crawler results, using Snowflake storage"""
    
    def __init__(self, config: dict = None):
        """Initialize with Snowflake configuration"""
        super().__init__(config)
        self.url_cache: Set[str] = set()
        self._initialized = False
    
    async def initialize(self):
        """Initialize database connections and cache"""
        if not self._initialized:
            try:
                # Initialize Snowflake connection
                await super().initialize()
                
                # Load URL cache
                cached_urls = await self.get_cached_urls()
                self.url_cache = set(cached_urls)
                
                self._initialized = True
                logger.info("Database initialized successfully")
                
            except Exception as e:
                logger.error(f"Database initialization failed: {str(e)}")
                raise
    
    async def save_file_path(self, url: str, file_type: str, file_path: Path, content_type: str = None, metadata: dict = None):
        """Save file metadata to Snowflake"""
        try:
            # First upload file to stage
            stage_path = f"{urlparse(url).netloc}/{file_type}/{file_path.name}"
            if await self.upload_to_stage(file_path, stage_path):
                # Then save metadata
                file_info = {
                    'url': url,
                    'file_name': file_path.name,
                    'file_type': file_type,
                    'size': file_path.stat().st_size,
                    'content_type': content_type,
                    'metadata': metadata
                }
                await self.save_file_metadata(url, file_info)
                logger.debug(f"Saved file metadata for {url}: {file_path}")
        except Exception as e:
            logger.error(f"Error saving file path for {url}: {e}")

    async def get_saved_files(self, url: str = None, file_type: str = None) -> List[Dict]:
        """Get saved file metadata from Snowflake"""
        try:
            query = "SELECT * FROM crawl_metadata"
            params = []
            
            if url or file_type:
                conditions = []
                if url:
                    conditions.append("url = %s")
                    params.append(url)
                if file_type:
                    conditions.append("file_type = %s")
                    params.append(file_type)
                query += " WHERE " + " AND ".join(conditions)
            
            query += " ORDER BY timestamp DESC"
            
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.execute(query, params)
                files = []
                async for row in cursor:
                    files.append({
                        'url': row[0],
                        'file_type': row[1],
                        'file_path': row[2],
                        'timestamp': row[3],
                        'content_type': row[4],
                        'size': row[5],
                        'metadata': json.loads(row[6]) if row[6] else {}
                    })
                return files
        except Exception as e:
            logger.error(f"Error getting saved files: {e}")
            return []
    
    async def get_cached_urls(self) -> List[str]:
        """Get list of all cached URLs from Snowflake"""
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.execute('SELECT url FROM crawl_metadata')
                return [row[0] async for row in cursor]
        except Exception as e:
            logger.error(f"Error getting cached URLs: {str(e)}")
            return []
    
    async def save_results(self, results: List[CrawlResult]):
        """Save crawl results to Snowflake"""
        await super().save_results(results)
        for result in results:
            if isinstance(result, CrawlResult):
                self.url_cache.add(result.url)
    
    async def get_result(self, url: str) -> Optional[CrawlResult]:
        """Get crawl result for specific URL from Snowflake"""
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.execute(
                    'SELECT * FROM crawl_metadata WHERE url = %s',
                    (url,)
                )
                row = await cursor.fetchone()
                if not row:
                    return None
                
                metadata = json.loads(row[3]) if row[3] else {}
                return CrawlResult(
                    url=row[0],
                    success=bool(row[1]),
                    error_message=row[2],
                    media=metadata.get('media', {}),
                    links=metadata.get('links', {}),
                    metadata=metadata.get('metadata', {})
                )
        except Exception as e:
            logger.error(f"Error getting result for {url}: {str(e)}")
            return None
    
    async def get_stats(self) -> Dict:
        """Get database statistics from Snowflake"""
        return await super().get_stats()