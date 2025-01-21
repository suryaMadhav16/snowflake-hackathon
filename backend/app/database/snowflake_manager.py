import logging
import json
import asyncio
import os
from typing import Dict, List, Optional, Union
from datetime import datetime
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
from core.config import settings

logger = logging.getLogger(__name__)

class SnowflakeManager:
    """Manages Snowflake connections and operations"""
    
    def __init__(self):
        """Initialize Snowflake manager"""
        self.config = {
            'user': settings.SNOWFLAKE_USER,
            'password': settings.SNOWFLAKE_PASSWORD,
            'account': settings.SNOWFLAKE_ACCOUNT,
            'warehouse': "Medium",
            'database': "LLM",
            'sfschema': "RAG",
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

    async def save_task(self, task: Dict) -> bool:
        """Save task state"""
        try:
            logger.debug(f"Saving task state: {task}")
            query = """
                MERGE INTO LLM.RAG.tasks t
                USING (SELECT %(task_id)s as task_id) s
                ON t.task_id = s.task_id
                WHEN MATCHED THEN
                    UPDATE SET
                        status = %(status)s,
                        progress = %(progress)s,
                        metrics = PARSE_JSON(%(metrics)s),
                        error = %(error)s,
                        current_url = %(current_url)s,
                        updated_at = CURRENT_TIMESTAMP(),
                        completed_at = CASE 
                            WHEN %(status)s IN ('completed', 'failed') 
                            THEN CURRENT_TIMESTAMP() 
                            ELSE completed_at 
                        END
                WHEN NOT MATCHED THEN
                    INSERT (
                        task_id, task_type, status, progress,
                        settings, metrics, error, current_url,
                        urls, created_at, updated_at
                    ) VALUES (
                        %(task_id)s, %(task_type)s, %(status)s, %(progress)s,
                        PARSE_JSON(%(settings)s), PARSE_JSON(%(metrics)s),
                        %(error)s, %(current_url)s, PARSE_JSON(%(urls)s),
                        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
                    )
            """
            
            params = {
                'task_id': task['task_id'],
                'task_type': task.get('task_type', 'crawl'),
                'status': task['status'],
                'progress': task.get('progress', 0.0),
                'settings': json.dumps(task.get('settings', {})),
                'metrics': json.dumps(task.get('metrics', {})),
                'error': task.get('error'),
                'current_url': task.get('current_url'),
                'urls': json.dumps(task.get('urls', []))
            }
            
            await self.execute_query(query, params, fetch=False)
            return True
        except Exception as e:
            logger.error(f"Error saving task: {str(e)}")
            return False

    async def get_task(self, task_id: str) -> Optional[Dict]:
        """Get task state from database"""
        try:
            logger.debug(f"Getting task state for {task_id}")
            query = """
                SELECT
                    task_id,
                    task_type,
                    status,
                    progress,
                    settings,
                    metrics,
                    error,
                    current_url,
                    urls,
                    created_at,
                    updated_at,
                    completed_at
                FROM LLM.RAG.tasks
                WHERE task_id = %(task_id)s
            """
            
            results = await self.execute_query(query, {'task_id': task_id})
            if results:
                task = results[0]
                logger.debug(f"Found task: {task}")
                return task
            logger.debug("Task not found")
            return None
        except Exception as e:
            logger.error(f"Error getting task: {str(e)}")
            return None

    async def list_active_tasks(self) -> List[Dict]:
        """Get all active tasks"""
        try:
            query = """
                SELECT
                    task_id,
                    task_type,
                    status,
                    progress,
                    metrics,
                    error,
                    current_url,
                    updated_at
                FROM LLM.RAG.tasks
                WHERE status NOT IN ('completed', 'failed')
                ORDER BY created_at DESC
            """
            
            return await self.execute_query(query)
        except Exception as e:
            logger.error(f"Error listing active tasks: {str(e)}")
            return []
    
    async def save_discovery_result(self, result: Dict) -> bool:
        """Save URL discovery result"""
        try:
            # First save task state
            task_state = {
                "task_id": result["task_id"],
                "task_type": "discovery",
                "status": "completed",
                "progress": 1.0,
                "metrics": {
                    "discovered_urls": len(result["discovered_urls"]),
                    "max_depth": result["max_depth"]
                },
                "urls": result["discovered_urls"],
                "completed_at": result["completed_at"]
            }
            await self.save_task(task_state)

            # Then save discovery details
            query = """
                MERGE INTO LLM.RAG.discovery_results t
                USING (SELECT %(task_id)s as task_id) s
                ON t.task_id = s.task_id
                WHEN MATCHED THEN 
                    UPDATE SET
                        start_url = %(start_url)s,
                        discovered_urls = PARSE_JSON(%(discovered_urls)s),
                        total_urls = %(total_urls)s,
                        max_depth = %(max_depth)s,
                        url_graph = PARSE_JSON(%(url_graph)s),
                        completed_at = %(completed_at)s
                WHEN NOT MATCHED THEN
                    INSERT (
                        task_id, start_url, discovered_urls,
                        total_urls, max_depth, url_graph,
                        created_at, completed_at
                    ) VALUES (
                        %(task_id)s, %(start_url)s, PARSE_JSON(%(discovered_urls)s),
                        %(total_urls)s, %(max_depth)s, PARSE_JSON(%(url_graph)s),
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
    
    async def save_crawl_result(self, result: Dict) -> bool:
        """Save crawl result to Snowflake"""
        try:
            query = """
                MERGE INTO LLM.RAG.crawl_results t 
                USING (SELECT %(url)s as url) s
                ON t.url = s.url
                WHEN MATCHED THEN 
                    UPDATE SET
                        success = %(success)s,
                        html = %(html)s,
                        cleaned_html = %(cleaned_html)s,
                        error_message = %(error_message)s,
                        media_data = PARSE_JSON(%(media_data)s),
                        links_data = PARSE_JSON(%(links_data)s),
                        metadata = PARSE_JSON(%(metadata)s)
                WHEN NOT MATCHED THEN
                    INSERT (
                        url, success, html, cleaned_html,
                        error_message, media_data, links_data, metadata
                    ) VALUES (
                        %(url)s, %(success)s, %(html)s, %(cleaned_html)s,
                        %(error_message)s, PARSE_JSON(%(media_data)s),
                        PARSE_JSON(%(links_data)s), PARSE_JSON(%(metadata)s)
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

    async def get_discovery_result(self, task_id: str) -> Optional[Dict]:
        """Get discovery result by task ID"""
        try:
            database = self.config['database']
            schema = self.config['sfschema']
            query = """
                SELECT 
                    task_id,
                    start_url,
                    discovered_urls,
                    total_urls,
                    max_depth,
                    url_graph,
                    created_at,
                    completed_at
                FROM LLM.RAG.discovery_results
                WHERE task_id = %(task_id)s
            """
            results = await self.execute_query(query, {'task_id': task_id})
            if results:
                result = results[0]
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting discovery result: {str(e)}")
            return None
    
    # async def save_crawl_result(self, result: Dict) -> bool:
    #     """Save crawl result to Snowflake"""
    #     try:
    #         query = """
    #             MERGE INTO RAG.LLM.crawl_results t 
    #             USING (SELECT %(url)s as url) s
    #             ON t.url = s.url
    #             WHEN MATCHED THEN 
    #                 UPDATE SET
    #                     success = %(success)s,
    #                     html = %(html)s,
    #                     cleaned_html = %(cleaned_html)s,
    #                     error_message = %(error_message)s,
    #                     media_data = PARSE_JSON(%(media_data)s),
    #                     links_data = PARSE_JSON(%(links_data)s),
    #                     metadata = PARSE_JSON(%(metadata)s)
    #             WHEN NOT MATCHED THEN
    #                 INSERT (
    #                     url, success, html, cleaned_html,
    #                     error_message, media_data, links_data, metadata
    #                 ) VALUES (
    #                     %(url)s, %(success)s, %(html)s, %(cleaned_html)s,
    #                     %(error_message)s, PARSE_JSON(%(media_data)s),
    #                     PARSE_JSON(%(links_data)s), PARSE_JSON(%(metadata)s)
    #                 )
    #         """
            
    #         params = {
    #             'url': result['url'],
    #             'success': result['success'],
    #             'html': result.get('html'),
    #             'cleaned_html': result.get('cleaned_html'),
    #             'error_message': result.get('error_message'),
    #             'media_data': json.dumps(result.get('media', {})),
    #             'links_data': json.dumps(result.get('links', {})),
    #             'metadata': json.dumps(result.get('metadata', {}))
    #         }
            
    #         await self.execute_query(query, params, fetch=False)
    #         return True
    #     except Exception as e:
    #         logger.error(f"Error saving crawl result: {str(e)}")
    #         return False
    
    async def get_crawl_result(self, url: str) -> Optional[Dict]:
        """Get crawl result for URL"""
        try:
            query = """
                SELECT 
                    url, success, html, cleaned_html,
                    error_message, media_data, links_data, metadata,
                    created_at
                FROM LLM.RAG.crawl_results 
                WHERE url = %(url)s
            """
            results = await self.execute_query(query, {'url': url})
            if results:
                result = results[0]
                return result
            return None
        except Exception as e:
            logger.error(f"Error getting crawl result: {str(e)}")
            return None
    
    async def save_file_info(self, file_info: Dict) -> bool:
        """Save file information to Snowflake"""
        try:
            query = """
                MERGE INTO RAG.LLM.saved_files t 
                USING (
                    SELECT 
                        %(url)s as url,
                        %(file_type)s as file_type,
                        %(stage_path)s as stage_path
                ) s
                ON t.url = s.url 
                    AND t.file_type = s.file_type 
                    AND t.stage_path = s.stage_path
                WHEN MATCHED THEN 
                    UPDATE SET
                        content_type = %(content_type)s,
                        size = %(size)s,
                        metadata = PARSE_JSON(%(metadata)s)
                WHEN NOT MATCHED THEN
                    INSERT (
                        url, file_type, stage_path,
                        content_type, size, metadata
                    ) VALUES (
                        %(url)s, %(file_type)s, %(stage_path)s,
                        %(content_type)s, %(size)s, PARSE_JSON(%(metadata)s)
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
    
    async def get_files(self, url: str, file_type: Optional[str] = None) -> List[Dict]:
        """Get file information"""
        try:
            query = """
                SELECT 
                    url, file_type, stage_path,
                    content_type, size, metadata,
                    created_at
                FROM RAG.LLM.saved_files
                WHERE url = %(url)s
            """
            params = {'url': url}
            
            if file_type:
                query += " AND file_type = %(file_type)s"
                params['file_type'] = file_type
            
            results = await self.execute_query(query, params)
            return results or []
        except Exception as e:
            logger.error(f"Error getting files: {str(e)}")
            return []
    
    async def save_task_metrics(self, task_id: str, metrics: Dict) -> bool:
        """Save task metrics snapshot"""
        try:
            query = """
                INSERT INTO LLM.RAG.task_metrics (
                    task_id, timestamp, metrics
                ) VALUES (
                    %(task_id)s, CURRENT_TIMESTAMP(), PARSE_JSON(%(metrics)s)
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
                    COUNT_IF(success) as successful_urls,
                    COUNT_IF(NOT success) as failed_urls
                FROM LLM.RAG.crawl_results
            """
            
            # Get discovery stats
            discovery_query = """
                SELECT 
                    COUNT(*) as total_tasks,
                    SUM(total_urls) as total_discovered_urls,
                    AVG(max_depth) as avg_depth
                FROM LLM.RAG.discovery_results
            """
            
            # Get file stats
            file_query = """
                SELECT 
                    file_type,
                    COUNT(*) as count,
                    SUM(size) as total_size
                FROM LLM.RAG.saved_files
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
    
    def close(self):
        """Close Snowflake connection"""
        if self._conn:
            try:
                self._conn.close()
                self._conn = None
            except Exception as e:
                logger.error(f"Error closing Snowflake connection: {str(e)}")

