import logging
import json
import asyncio
from typing import Dict, List, Optional, Union
from datetime import datetime
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
from ..core.config import settings

logger = logging.getLogger(__name__)

class SnowflakeManager:
    """Manages Snowflake connections and operations"""
    
    def __init__(self):
        self.config = {
            'user': settings.SNOWFLAKE_USER,
            'password': settings.SNOWFLAKE_PASSWORD,
            'account': settings.SNOWFLAKE_ACCOUNT,
            'warehouse': settings.SNOWFLAKE_WAREHOUSE,
            'database': settings.SNOWFLAKE_DATABASE,
            'schema': settings.SNOWFLAKE_SCHEMA,
            'role': settings.SNOWFLAKE_ROLE
        }
        self._conn = None
        self._lock = asyncio.Lock()
    
    async def get_connection(self):
        """Get or create Snowflake connection"""
        if not self._conn:
            async with self._lock:
                if not self._conn:
                    try:
                        self._conn = snowflake.connector.connect(**self.config)
                        logger.info("Connected to Snowflake successfully")
                    except Exception as e:
                        logger.error(f"Failed to connect to Snowflake: {str(e)}")
                        raise
        return self._conn
    
    async def execute_query(
        self,
        query: str,
        params: Dict = None,
        fetch: bool = True
    ) -> Union[List[Dict], None]:
        """Execute SQL query"""
        conn = await self.get_connection()
        try:
            cur = conn.cursor()
            cur.execute(query, params or {})
            
            if fetch and cur.description:
                columns = [col[0] for col in cur.description]
                results = []
                for row in cur:
                    results.append(dict(zip(columns, row)))
                return results
            
            return None
            
        except Exception as e:
            logger.error(f"Query execution error: {str(e)}")
            raise
        finally:
            if 'cur' in locals():
                cur.close()
    
    async def initialize_tables(self):
        """Initialize database tables"""
        try:
            # Create discovery_results table
            await self.execute_query("""
                CREATE TABLE IF NOT EXISTS discovery_results (
                    task_id STRING PRIMARY KEY,
                    start_url STRING,
                    discovered_urls ARRAY,
                    total_urls INTEGER,
                    max_depth INTEGER,
                    url_graph VARIANT,
                    created_at TIMESTAMP_NTZ,
                    completed_at TIMESTAMP_NTZ
                )
            """)
            
            # Create crawl_results table
            await self.execute_query("""
                CREATE TABLE IF NOT EXISTS crawl_results (
                    url STRING PRIMARY KEY,
                    success BOOLEAN,
                    html STRING,
                    cleaned_html STRING,
                    error_message STRING,
                    media_data VARIANT,
                    links_data VARIANT,
                    metadata VARIANT,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            
            # Create saved_files table
            await self.execute_query("""
                CREATE TABLE IF NOT EXISTS saved_files (
                    url STRING,
                    file_type STRING,
                    stage_path STRING,
                    content_type STRING,
                    size NUMBER,
                    metadata VARIANT,
                    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (url, file_type, stage_path)
                )
            """)
            
            # Create task_metrics table
            await self.execute_query("""
                CREATE TABLE IF NOT EXISTS task_metrics (
                    task_id STRING,
                    timestamp TIMESTAMP_NTZ,
                    metrics VARIANT,
                    PRIMARY KEY (task_id, timestamp)
                )
            """)
            
            logger.info("Database tables initialized successfully")
        except Exception as e:
            logger.error(f"Error initializing tables: {str(e)}")
            raise
    
    async def save_discovery_result(self, result: Dict) -> bool:
        """Save URL discovery result"""
        try:
            query = """
                INSERT INTO discovery_results (
                    task_id, start_url, discovered_urls,
                    total_urls, max_depth, url_graph,
                    created_at, completed_at
                ) VALUES (
                    %(task_id)s, %(start_url)s, %(discovered_urls)s,
                    %(total_urls)s, %(max_depth)s, %(url_graph)s,
                    %(created_at)s, %(completed_at)s
                )
            """
            
            params = {
                'task_id': result['task_id'],
                'start_url': result['start_url'],
                'discovered_urls': json.dumps(result['discovered_urls']),
                'total_urls': result['total_urls'],
                'max_depth': result['max_depth'],
                'url_graph': json.dumps(result['url_graph']),
                'created_at': result['created_at'],
                'completed_at': result['completed_at']
            }
            
            await self.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error saving discovery result: {str(e)}")
            return False
    
    async def get_discovery_result(self, task_id: str) -> Optional[Dict]:
        """Get discovery result by task ID"""
        try:
            query = """
                SELECT *
                FROM discovery_results
                WHERE task_id = %(task_id)s
            """
            results = await self.execute_query(query, {'task_id': task_id})
            if results:
                result = results[0]
                result['discovered_urls'] = json.loads(result['discovered_urls'])
                result['url_graph'] = json.loads(result['url_graph'])
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting discovery result: {str(e)}")
            return None
    
    async def save_crawl_result(self, result: Dict) -> bool:
        """Save crawl result to Snowflake"""
        try:
            query = """
                INSERT INTO crawl_results (
                    url, success, html, cleaned_html,
                    error_message, media_data, links_data, metadata,
                    created_at
                ) VALUES (
                    %(url)s, %(success)s, %(html)s, %(cleaned_html)s,
                    %(error_message)s, %(media_data)s, %(links_data)s, %(metadata)s,
                    CURRENT_TIMESTAMP
                )
            """
            
            params = {
                'url': result['url'],
                'success': result['success'],
                'html': result.get('html'),
                'cleaned_html': result.get('cleaned_html'),
                'error_message': result.get('error_message'),
                'media_data': json.dumps(result.get('media', {})),
                'links_data': json.dumps(result.get('links', {})),
                'metadata': json.dumps(result.get('metadata', {}))
            }
            
            await self.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error saving crawl result: {str(e)}")
            return False
    
    async def get_crawl_result(self, url: str) -> Optional[Dict]:
        """Get crawl result for URL"""
        try:
            query = "SELECT * FROM crawl_results WHERE url = %(url)s"
            results = await self.execute_query(query, {'url': url})
            if results:
                result = results[0]
                result['media_data'] = json.loads(result['media_data'])
                result['links_data'] = json.loads(result['links_data'])
                result['metadata'] = json.loads(result['metadata'])
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting crawl result: {str(e)}")
            return None
    
    async def save_file_info(self, file_info: Dict) -> bool:
        """Save file information to Snowflake"""
        try:
            query = """
                INSERT INTO saved_files (
                    url, file_type, stage_path, content_type,
                    size, metadata, created_at
                ) VALUES (
                    %(url)s, %(file_type)s, %(stage_path)s, %(content_type)s,
                    %(size)s, %(metadata)s, CURRENT_TIMESTAMP
                )
            """
            
            params = {
                'url': file_info['url'],
                'file_type': file_info['file_type'],
                'stage_path': file_info['stage_path'],
                'content_type': file_info.get('content_type'),
                'size': file_info.get('size', 0),
                'metadata': json.dumps(file_info.get('metadata', {}))
            }
            
            await self.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error saving file info: {str(e)}")
            return False
    
    async def save_task_metrics(self, task_id: str, metrics: Dict) -> bool:
        """Save task metrics snapshot"""
        try:
            query = """
                INSERT INTO task_metrics (
                    task_id, timestamp, metrics
                ) VALUES (
                    %(task_id)s, CURRENT_TIMESTAMP, %(metrics)s
                )
            """
            
            params = {
                'task_id': task_id,
                'metrics': json.dumps(metrics)
            }
            
            await self.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error saving task metrics: {str(e)}")
            return False
    
    async def get_stats(self) -> Dict:
        """Get crawler statistics from Snowflake"""
        try:
            # Get crawl stats
            crawl_query = """
                SELECT 
                    COUNT(*) as total_urls,
                    SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_urls,
                    SUM(CASE WHEN NOT success THEN 1 ELSE 0 END) as failed_urls
                FROM crawl_results
            """
            
            # Get discovery stats
            discovery_query = """
                SELECT 
                    COUNT(*) as total_tasks,
                    SUM(total_urls) as total_discovered_urls,
                    AVG(max_depth) as avg_depth
                FROM discovery_results
            """
            
            # Get file stats
            file_query = """
                SELECT 
                    file_type,
                    COUNT(*) as count,
                    SUM(size) as total_size
                FROM saved_files
                GROUP BY file_type
            """
            
            crawl_results = await self.execute_query(crawl_query)
            discovery_results = await self.execute_query(discovery_query)
            file_results = await self.execute_query(file_query)
            
            crawl_stats = crawl_results[0] if crawl_results else {
                'total_urls': 0,
                'successful_urls': 0,
                'failed_urls': 0
            }
            
            discovery_stats = discovery_results[0] if discovery_results else {
                'total_tasks': 0,
                'total_discovered_urls': 0,
                'avg_depth': 0
            }
            
            file_stats = {}
            if file_results:
                for row in file_results:
                    file_stats[row['file_type']] = {
                        'count': row['count'],
                        'total_size': row['total_size']
                    }
            
            return {
                'crawl_stats': crawl_stats,
                'discovery_stats': discovery_stats,
                'file_stats': file_stats,
                'last_update': datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {}