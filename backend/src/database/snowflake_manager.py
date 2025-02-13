import os
import json
import logging
import asyncio
from typing import Dict, List, Optional, Union
from datetime import datetime
from pathlib import Path
import snowflake.connector
from snowflake.connector.errors import ProgrammingError, DatabaseError
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
from functools import partial

# Setup detailed logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def normalize_response(data: Dict) -> Dict:
    """Convert response keys to uppercase"""
    if not data:
        return {}
    return {k.upper(): v for k, v in data.items()}

class SnowflakeManager:
    """Manages Snowflake database operations for crawler results"""
    
    def __init__(self, config: dict = None):
        """Initialize Snowflake connection"""
        logger.info("Initializing Snowflake connection...")
        self.config = config or {
            'user': os.getenv('SNOWFLAKE_USER'),
            'password': os.getenv('SNOWFLAKE_PASSWORD'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT'),
            'warehouse': 'MEDIUM',
            'database': 'LLM',
            'schema': 'RAG',
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        }
        logger.info(f"Using configuration: {self.config.copy().update({'password': '*****'})}")
        self._conn = None
        self._lock = asyncio.Lock()
        
        # Store fully qualified names
        self.database = 'LLM'
        self.schema = 'RAG'
    
    async def _get_connection(self):
        """Get or create Snowflake connection"""
        async with self._lock:
            if not self._conn:
                try:
                    logger.info("Creating new Snowflake connection...")
                    loop = asyncio.get_event_loop()
                    self._conn = await loop.run_in_executor(
                        None,
                        lambda: snowflake.connector.connect(**self.config)
                    )
                    logger.info("Successfully created Snowflake connection")
                except Exception as e:
                    logger.error(f"Failed to connect to Snowflake: {str(e)}")
                    raise
        return self._conn
    
    async def _execute_query(
        self,
        query: str,
        params: Dict = None,
        fetch: bool = True
    ) -> Union[List[Dict], None]:
        """Execute SQL query asynchronously"""
        conn = await self._get_connection()
        cur = None
        try:
            loop = asyncio.get_event_loop()
            cur = conn.cursor()
            
            # Log the query with parameters
            formatted_query = query
            if params:
                # Safely format query for logging
                for k, v in params.items():
                    formatted_query = formatted_query.replace(f'%({k})s', repr(v))
            logger.info(f"Executing query: {formatted_query}")
            
            # Execute query
            await loop.run_in_executor(None, lambda: cur.execute(query, params or {}))
            
            if fetch and cur.description:
                rows = await loop.run_in_executor(None, cur.fetchall)
                columns = [col[0].upper() for col in cur.description]
                results = [normalize_response(dict(zip(columns, row))) for row in rows]
                logger.debug(f"Query returned {len(results)} rows")
                
                # Special handling for stored procedure results
                if query.strip().upper().startswith('CALL'):
                    if results and len(results) > 0:
                        proc_result = results[0]
                        # Handle SYNC_CRAWL_CONTENT procedure specifically
                        if 'SYNC_CRAWL_CONTENT' in query:
                            result_obj = proc_result.get('SYNC_CRAWL_CONTENT')
                            # Handle string results
                            if isinstance(result_obj, str):
                                logger.info(f"Procedure completed with result: {result_obj}")
                            # Handle dictionary results
                            elif isinstance(result_obj, dict):
                                if result_obj.get('status') == 'error':
                                    error_info = result_obj.get('error', {})
                                    error_msg = f"Stored procedure error: {error_info.get('message', 'Unknown error')}"
                                    logger.error(error_msg)
                                    raise Exception(error_msg)
                                elif result_obj.get('status') == 'success':
                                    logger.info(f"Procedure successful: {result_obj.get('message')}")
                            else:
                                logger.info(f"Procedure completed with unexpected result type: {type(result_obj)}")
                
                return results
            
            return None
            
        except Exception as e:
            logger.error(f"Query execution error for query '{formatted_query}': {str(e)}")
            raise
        finally:
            if cur:
                await loop.run_in_executor(None, cur.close)

    async def list_tables(self):
        """List all tables and their columns in the current schema"""
        try:
            query = f"""
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable
                FROM {self.database}.INFORMATION_SCHEMA.COLUMNS
                WHERE table_schema = '{self.schema}'
                ORDER BY table_name, ordinal_position
            """
            
            logger.info(f"Fetching schema information for {self.database}.{self.schema}")
            results = await self._execute_query(query)
            
            if not results:
                logger.warning(f"No tables found in {self.database}.{self.schema}")
                return
            
            # Group by table
            tables = {}
            for row in results:
                table = row['TABLE_NAME']
                if table not in tables:
                    tables[table] = []
                tables[table].append({
                    'column': row['COLUMN_NAME'],
                    'type': row['DATA_TYPE'],
                    'nullable': row['IS_NULLABLE']
                })
            
            # Log table information
            logger.info(f"Found {len(tables)} tables in {self.database}.{self.schema}:")
            for table, columns in tables.items():
                logger.info(f"\nTable: {table}")
                for col in columns:
                    logger.info(f"  - {col['column']} ({col['type']}) {'NULL' if col['nullable'] == 'YES' else 'NOT NULL'}")
                    
            return tables
            
        except Exception as e:
            logger.error(f"Error listing tables: {str(e)}")
            raise

    async def initialize(self):
        """Initialize database connection and setup"""
        try:
            conn = await self._get_connection()
            loop = asyncio.get_event_loop()
            
            async def exec_command(cmd, description):
                cur = conn.cursor()
                try:
                    logger.info(f"Executing: {description}")
                    logger.debug(f"SQL: {cmd}")
                    await loop.run_in_executor(None, lambda: cur.execute(cmd))
                    logger.info(f"Successfully executed: {description}")
                finally:
                    cur.close()
            
            # Execute setup commands
            await exec_command("USE WAREHOUSE MEDIUM", "Set warehouse")
            await exec_command(f"USE DATABASE {self.database}", "Set database")
            await exec_command(f"USE SCHEMA {self.schema}", "Set schema")
            
            # List tables and their structure
            await self.list_tables()
            
            logger.info("Snowflake connection initialized successfully")
            
        except Exception as e:
            logger.error(f"Snowflake initialization failed: {str(e)}")
            raise

    async def upload_to_stage(self, file_path: Path, stage_path: str) -> bool:
        """Upload file to Snowflake stage"""
        cur = None
        try:
            stage_path = stage_path.replace('\\', '/')
            file_path = str(file_path).replace('\\', '/')
            
            conn = await self._get_connection()
            loop = asyncio.get_event_loop()
            
            put_command = f"PUT 'file://{file_path}' @{self.database}.{self.schema}.DOCUMENTATIONS/{stage_path} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            logger.info(f"Uploading file to stage: {file_path}")
            logger.debug(f"PUT command: {put_command}")
            
            cur = conn.cursor()
            await loop.run_in_executor(None, lambda: cur.execute(put_command))
            result = await loop.run_in_executor(None, cur.fetchall)
            
            status = result[0][6] if result and len(result[0]) > 6 else ''
            success = 'UPLOADED' in status.upper()
            
            if success:
                logger.info(f"Successfully uploaded {file_path} to @DOCUMENTATIONS/{stage_path}")
            else:
                logger.error(f"Failed to upload {file_path}: {status}")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to upload to stage: {str(e)}")
            return False
        finally:
            if cur:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, cur.close)

    async def save_file_metadata(self, url: str, file_info: Dict) -> bool:
        """Save file metadata to CRAWL_METADATA table"""
        try:
            metadata = normalize_response(file_info.get('metadata', {})) if file_info.get('metadata') else {}
            file_info = normalize_response(file_info)
            
            if 'stage_path' in metadata:
                metadata.update({
                    'STAGE_PATH': metadata['stage_path'],
                    'FILE_TYPE_META': file_info.get('FILE_TYPE')
                })
            
            query = f"""
                MERGE INTO {self.database}.{self.schema}.CRAWL_METADATA t
                USING (SELECT %(url)s as URL) s
                ON t.URL = s.URL
                WHEN MATCHED THEN
                    UPDATE SET
                        FILE_NAME = %(file_name)s,
                        FILE_TYPE = %(file_type)s,
                        CONTENT_TYPE = %(content_type)s,
                        SIZE = %(size)s,
                        METADATA = PARSE_JSON(%(metadata)s),
                        TIMESTAMP = CURRENT_TIMESTAMP()
                WHEN NOT MATCHED THEN
                    INSERT (URL, FILE_NAME, FILE_TYPE, CONTENT_TYPE, SIZE, METADATA)
                    VALUES (
                        %(url)s, %(file_name)s, %(file_type)s, %(content_type)s,
                        %(size)s, PARSE_JSON(%(metadata)s)
                    )
            """
            
            params = {
                'url': url,
                'file_name': file_info['FILE_NAME'],
                'file_type': file_info['FILE_TYPE'],
                'content_type': file_info['CONTENT_TYPE'],
                'size': file_info['SIZE'],
                'metadata': json.dumps(metadata)
            }
            
            await self._execute_query(query, params, fetch=False)
            logger.info(f"Successfully saved metadata for {url}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to save file metadata: {str(e)}")
            return False

    async def save_results(self, results: List):
        """Save crawl results to Snowflake"""
        try:
            logger.info(f"Saving {len(results)} crawl results")
            results_data = []
            
            for result in results:
                metadata = {
                    'MEDIA': getattr(result, 'media', {}) or {},
                    'LINKS': getattr(result, 'links', {}) or {},
                    'METADATA': getattr(result, 'metadata', {}) or {},                    
                }
                content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else result.markdown
                results_data.append({
                    'URL': result.url,
                    'SUCCESS': result.success,
                    'ERROR_MESSAGE': result.error_message if hasattr(result, 'error_message') else None,
                    'METADATA': json.dumps(metadata),                    
                    'MARKDOWN': content,
                })
            
            if results_data:
                df = pd.DataFrame(results_data)
                conn = await self._get_connection()
                
                table_name = 'CRAWL_METADATA'
                logger.info(f"Writing {len(df)} rows to {table_name}")
                
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: write_pandas(conn, df, table_name)
                )
                
                
                
                
                
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
            raise

    async def get_stats(self) -> Dict:
        """Get crawler statistics from Snowflake"""
        try:
            logger.info("Fetching crawler statistics")
            crawl_query = f"""
                SELECT 
                    COUNT(*) as TOTAL_URLS,
                    COUNT_IF(SUCCESS) as SUCCESSFUL_URLS,
                    COUNT_IF(NOT SUCCESS) as FAILED_URLS
                FROM {self.database}.{self.schema}.CRAWL_METADATA
            """
            
            file_query = f"""
                SELECT 
                    FILE_TYPE,
                    COUNT(*) as COUNT,
                    SUM(SIZE) as TOTAL_SIZE
                FROM {self.database}.{self.schema}.CRAWL_METADATA
                WHERE FILE_TYPE IS NOT NULL
                GROUP BY FILE_TYPE
            """
            
            crawl_results = await self._execute_query(crawl_query)
            file_results = await self._execute_query(file_query)
            
            crawl_stats = normalize_response(crawl_results[0]) if crawl_results else {
                'TOTAL_URLS': 0,
                'SUCCESSFUL_URLS': 0,
                'FAILED_URLS': 0
            }
            
            file_stats = {}
            if file_results:
                for row in file_results:
                    row = normalize_response(row)
                    file_stats[row['FILE_TYPE']] = {
                        'COUNT': row['COUNT'],
                        'TOTAL_SIZE': row['TOTAL_SIZE']
                    }
            
            stats = {
                'CRAWL_STATS': crawl_stats,
                'FILE_STATS': file_stats,
                'LAST_UPDATE': datetime.now().isoformat()
            }
            logger.info(f"Statistics: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats: {str(e)}")
            return {}
    
    
    async def close(self):
        """Close Snowflake connection"""
        if self._conn:
            try:
                logger.info("Closing Snowflake connection")
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self._conn.close)
                self._conn = None
                logger.info("Snowflake connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing Snowflake connection: {str(e)}")
                
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        await self.close()
        
    def __del__(self):
        """Destructor to ensure connection cleanup"""
        if self._conn:
            self._conn.close()
