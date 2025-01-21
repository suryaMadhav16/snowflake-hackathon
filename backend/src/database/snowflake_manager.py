import os
import json
import logging
import asyncio
from typing import Dict, List, Optional
from datetime import datetime
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas
from snowflake.connector import SnowflakeConnection
import pandas as pd
from crawl4ai import CrawlResult

logger = logging.getLogger(__name__)

class SnowflakeManager:
    """Manages Snowflake database operations for crawler results"""
    
    def __init__(self, config: dict = None):
        """Initialize Snowflake connection"""
        logger.info("==================Initializing Snowflake connection================")
        self.config = config or {
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'warehouse': 'MEDIUM',
            'database': 'LLM',
            'schema': 'RAG'
        }
        self.config['schema'] = 'RAG'
        self.config['database'] = 'LLM'
        self.config['warehouse'] = 'MEDIUM'
        logger.info(self.config)
        self._lock = asyncio.Lock()
        self._conn = None
        
    async def _get_connection(self) -> SnowflakeConnection:
        """Get or create Snowflake connection"""
        if not self._conn:
            ss = SnowflakeConnection()
            logger.info("================Connecting to Snowflake...===========")
            logger.info(self.config)
            self._conn = await ss.connect(**self.config)
        return self._conn
        
    async def initialize(self):
        """Initialize database connection and setup"""
        try:
            conn = await self._get_connection()
            # Use existing stage and tables
            await conn.cursor().execute("USE WAREHOUSE MEDIUM")
            await conn.cursor().execute("USE DATABASE LLM")
            await conn.cursor().execute("USE SCHEMA RAG")
            logger.info("Snowflake connection initialized successfully")
        except Exception as e:
            logger.error(f"Snowflake initialization failed: {str(e)}")
            raise

    async def upload_to_stage(self, file_path: Path, stage_path: str) -> bool:
        """Upload file to Snowflake stage"""
        try:
            conn = await self._get_connection()
            put_command = f"PUT 'file://{file_path}' @DOCUMENTATIONS/{stage_path} AUTO_COMPRESS=FALSE"
            async with conn.cursor() as cursor:
                await cursor.execute(put_command)
            return True
        except Exception as e:
            logger.error(f"Failed to upload to stage: {str(e)}")
            return False

    async def save_file_metadata(self, url: str, file_info: Dict) -> bool:
        """Save file metadata to crawl_metadata table"""
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cursor:
                await cursor.execute("""
                    INSERT INTO crawl_metadata (
                        url, file_name, file_type, content_type, size, metadata
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s
                    )
                """, (
                    url,
                    file_info['file_name'],
                    file_info['file_type'],
                    file_info['content_type'],
                    file_info['size'],
                    json.dumps(file_info.get('metadata', {}))
                ))
            return True
        except Exception as e:
            logger.error(f"Failed to save file metadata: {str(e)}")
            return False

    async def save_results(self, results: List[CrawlResult]):
        """Save crawl results to Snowflake"""
        try:
            # Convert results to pandas DataFrame for efficient upload
            results_data = []
            for result in results:
                if not isinstance(result, CrawlResult):
                    continue
                    
                results_data.append({
                    'url': result.url,
                    'success': result.success,
                    'error_message': result.error_message,
                    'metadata': json.dumps({
                        'media': result.media or {},
                        'links': result.links or {},
                        'metadata': getattr(result, 'metadata', {}) or {}
                    }),
                    'timestamp': datetime.now()
                })
                
            if results_data:
                df = pd.DataFrame(results_data)
                conn = await self._get_connection()
                # Use write_pandas for efficient batch upload
                write_pandas(conn, df, 'crawl_metadata')
                
                # Trigger sync procedure to update documentations table
                async with conn.cursor() as cursor:
                    await cursor.execute("CALL sync_crawl_content()")
                
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")

    async def get_stats(self) -> Dict:
        """Get crawl statistics from Snowflake"""
        try:
            conn = await self._get_connection()
            stats = {}
            
            async with conn.cursor() as cursor:
                # Get crawl stats
                await cursor.execute("""
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful,
                        SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed
                    FROM crawl_metadata
                """)
                row = await cursor.fetchone()
                stats['crawl_stats'] = {
                    'total_urls': row[0] or 0,
                    'successful': row[1] or 0,
                    'failed': row[2] or 0
                }
                
                # Get file stats
                await cursor.execute("""
                    SELECT file_type, COUNT(*) as count, SUM(size) as total_size
                    FROM crawl_metadata
                    WHERE file_type IS NOT NULL
                    GROUP BY file_type
                """)
                file_stats = {}
                async for row in cursor:
                    file_stats[row[0]] = {
                        'count': row[1],
                        'total_size': row[2]
                    }
                stats['file_stats'] = file_stats
                
            return stats
        except Exception as e:
            logger.error(f"Error getting statistics: {str(e)}")
            return {}