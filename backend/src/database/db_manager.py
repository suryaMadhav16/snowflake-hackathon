import json
import logging
from typing import Dict, List, Set, Optional
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse
from crawl4ai import CrawlResult
from .snowflake_manager import SnowflakeManager, normalize_response

logger = logging.getLogger(__name__)

class DatabaseManager(SnowflakeManager):
    """Manages database operations for crawler results, using Snowflake storage"""
    
    def __init__(self, config: dict = None):
        """Initialize with Snowflake configuration"""
        logger.info("Initializing DatabaseManager")
        super().__init__(config)
        self.url_cache: Set[str] = set()
        self._initialized = False
    
    async def initialize(self):
        """Initialize database and cache"""
        if not self._initialized:
            try:
                logger.info("Starting DatabaseManager initialization")
                
                # Initialize Snowflake connection
                await super().initialize()
                
                # Load URL cache from Snowflake
                logger.info("Loading URL cache")
                cached_urls = await self.get_cached_urls()
                self.url_cache = set(cached_urls)
                logger.info(f"Loaded {len(self.url_cache)} cached URLs")
                
                self._initialized = True
                logger.info("DatabaseManager initialized successfully")
                
            except Exception as e:
                logger.error(f"Database initialization failed: {str(e)}")
                raise
    
    async def save_file_path(self, url: str, file_type: str, file_path: Path, content_type: str = None, metadata: dict = None):
        """Save file metadata and upload to Snowflake stage"""
        try:
            logger.info(f"Saving file path for URL: {url}, Type: {file_type}")
            
            # Generate stage path
            domain = urlparse(url).netloc
            stage_path = f"{domain}/{file_type}/{file_path.name}"
            logger.debug(f"Generated stage path: {stage_path}")
            
            # First upload file to stage
            logger.info(f"Uploading file to stage: {file_path}")
            if await self.upload_to_stage(file_path, stage_path):
                metadata = metadata or {}
                metadata['STAGE_PATH'] = stage_path
                
                file_info = {
                    'URL': url,
                    'FILE_NAME': file_path.name,
                    'FILE_TYPE': file_type,
                    'SIZE': file_path.stat().st_size if file_path.exists() else 0,
                    'CONTENT_TYPE': content_type,
                    'METADATA': metadata
                }
                logger.info(f"File uploaded successfully, saving metadata")
                return await self.save_file_metadata(url, file_info)
            
            logger.error(f"Failed to upload file to stage: {file_path}")
            return False
            
        except Exception as e:
            logger.error(f"Error saving file path for {url}: {e}")
            return False

    async def get_saved_files(self, url: str = None, file_type: str = None) -> List[Dict]:
        """Get saved file metadata from Snowflake"""
        try:
            logger.info(f"Getting saved files - URL: {url}, Type: {file_type}")
            
            query = f"""
                SELECT URL, FILE_NAME, FILE_TYPE, CONTENT_TYPE, SIZE, METADATA
                FROM {self.database}.{self.schema}.CRAWL_METADATA
                WHERE 1=1
            """
            params = {}
            
            if url:
                query += " AND URL = %(url)s"
                params['url'] = url
            if file_type:
                query += " AND FILE_TYPE = %(file_type)s"
                params['file_type'] = file_type
            
            query += " ORDER BY TIMESTAMP DESC"
            
            results = await self._execute_query(query, params)
            
            if not results:
                logger.info("No saved files found")
                return []
            
            logger.info(f"Found {len(results)} saved files")
            
            # Process metadata JSON and convert stage paths
            processed_results = []
            for row in results:
                row = normalize_response(row)
                metadata = json.loads(row.get('METADATA', '{}')) if row.get('METADATA') else {}
                if 'STAGE_PATH' in metadata:
                    row['FILE_PATH'] = f"@{self.database}.{self.schema}.DOCUMENTATIONS/{metadata['STAGE_PATH']}"
                processed_results.append(row)
            
            return processed_results
            
        except Exception as e:
            logger.error(f"Error getting saved files: {e}")
            return []
    
    async def get_cached_urls(self) -> List[str]:
        """Get list of all cached URLs from Snowflake"""
        try:
            logger.info("Fetching cached URLs")
            query = f"""
                SELECT DISTINCT URL 
                FROM {self.database}.{self.schema}.CRAWL_METADATA
            """
            results = await self._execute_query(query)
            urls = [r['URL'] for r in results] if results else []
            logger.info(f"Found {len(urls)} cached URLs")
            return urls
        except Exception as e:
            logger.error(f"Error getting cached URLs: {str(e)}")
            return []
    
    async def save_results(self, results: List[CrawlResult]):
        """Save crawl results to Snowflake"""
        try:
            logger.info(f"Saving {len(results)} crawl results")
            # Call parent method to save to Snowflake
            await super().save_results(results)
            
            # Update local cache
            for result in results:
                if isinstance(result, CrawlResult):
                    self.url_cache.add(result.url)
                    
            logger.info("Successfully saved results and updated cache")
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
    
    async def get_result(self, url: str) -> Optional[CrawlResult]:
        """Get crawl result for specific URL from Snowflake"""
        try:
            logger.info(f"Getting crawl result for URL: {url}")
            
            query = f"""
                SELECT URL, SUCCESS, ERROR_MESSAGE, METADATA
                FROM {self.database}.{self.schema}.CRAWL_METADATA 
                WHERE URL = %(url)s
            """
            results = await self._execute_query(query, {'url': url})
            
            if not results:
                logger.info(f"No result found for URL: {url}")
                return None
            
            row = normalize_response(results[0])
            metadata = json.loads(row.get('METADATA', '{}')) if row.get('METADATA') else {}
            
            logger.info(f"Found result for URL: {url}")
            
            # Extract components from metadata
            return CrawlResult(
                url=row['URL'],
                success=bool(row['SUCCESS']),
                error_message=row.get('ERROR_MESSAGE'),
                media=metadata.get('MEDIA', {}),
                links=metadata.get('LINKS', {}),
                metadata=metadata.get('METADATA', {})
            )
        except Exception as e:
            logger.error(f"Error getting result for {url}: {str(e)}")
            return None
    
    async def get_stats(self) -> Dict:
        """Get database statistics from Snowflake"""
        logger.info("Fetching database statistics")
        return await super().get_stats()
    
    async def __aenter__(self):
        """Async context manager entry"""
        logger.info("Entering DatabaseManager context")
        await self.initialize()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        logger.info("Exiting DatabaseManager context")
        await self.close()
    
    def __del__(self):
        """Destructor to ensure connection cleanup"""
        if hasattr(self, '_conn') and self._conn:
            logger.info("Cleaning up DatabaseManager connection")
            self._conn.close()