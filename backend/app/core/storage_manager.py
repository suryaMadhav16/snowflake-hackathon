import logging
import hashlib
import tempfile
from pathlib import Path
from typing import Dict, Optional, Union, BinaryIO, List
from database.snowflake_manager import SnowflakeManager

logger = logging.getLogger(__name__)

class StorageManager:
    """Manages file storage in Snowflake Stage"""
    
    def __init__(self, snowflake: SnowflakeManager):
        self.snowflake = snowflake
        self.stage_name = "@crawled_content"
    
    def _generate_stage_path(self, url: str, file_type: str) -> str:
        """Generate unique stage path for file"""
        hash_value = hashlib.sha256(url.encode()).hexdigest()[:12]
        return f"{self.stage_name}/{file_type}/{hash_value}"
    
    async def _create_temp_file(self, content: Union[str, bytes]) -> Path:
        """Create temporary file with content"""
        with tempfile.NamedTemporaryFile(delete=False) as temp:
            if isinstance(content, str):
                temp.write(content.encode('utf-8'))
            else:
                temp.write(content)
            return Path(temp.name)
    
    async def save_file(
        self,
        url: str,
        file_type: str,
        content: Union[str, bytes],
        content_type: str = None,
        metadata: Dict = None
    ) -> Dict[str, str]:
        """Save file to stage and record in database"""
        try:
            # Generate stage path
            stage_path = self._generate_stage_path(url, file_type)
            
            # Create temporary file
            temp_file = await self._create_temp_file(content)
            
            try:
                # Upload to stage
                query = f"""
                PUT file://{temp_file} {stage_path}
                AUTO_COMPRESS = TRUE
                OVERWRITE = TRUE
                """
                await self.snowflake.execute_query(query)
                
                # Record in database
                file_info = {
                    "url": url,
                    "file_type": file_type,
                    "stage_path": stage_path,
                    "content_type": content_type,
                    "size": len(content),
                    "metadata": metadata
                }
                
                await self.snowflake.save_file_info(file_info)
                return file_info
                
            finally:
                # Clean up temporary file
                temp_file.unlink()
                
        except Exception as e:
            logger.error(f"Error saving file: {str(e)}")
            raise
    
    async def get_file(
        self,
        url: str,
        file_type: str
    ) -> Optional[bytes]:
        """Retrieve file from stage"""
        try:
            # Get file info from database
            query = """
            SELECT stage_path
            FROM saved_files
            WHERE url = %(url)s AND file_type = %(file_type)s
            LIMIT 1
            """
            results = await self.snowflake.execute_query(
                query,
                {"url": url, "file_type": file_type}
            )
            
            if not results:
                return None
            
            stage_path = results[0]["stage_path"]
            
            # Get from stage
            with tempfile.NamedTemporaryFile() as temp:
                get_query = f"""
                GET {stage_path}
                FILE_FORMAT = (TYPE = BINARY)
                """
                await self.snowflake.execute_query(get_query)
                
                # Read content
                with open(temp.name, 'rb') as f:
                    return f.read()
                    
        except Exception as e:
            logger.error(f"Error retrieving file: {str(e)}")
            return None
    
    async def list_files(
        self,
        url: str = None,
        file_type: str = None
    ) -> List[Dict]:
        """List files in stage"""
        try:
            # Build query
            query = "SELECT * FROM saved_files WHERE 1=1"
            params = {}
            
            if url:
                query += " AND url = %(url)s"
                params["url"] = url
            
            if file_type:
                query += " AND file_type = %(file_type)s"
                params["file_type"] = file_type
                
            return await self.snowflake.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error listing files: {str(e)}")
            return []
    
    async def delete_file(
        self,
        url: str,
        file_type: str
    ) -> bool:
        """Delete file from stage"""
        try:
            # Get file info
            query = """
            SELECT stage_path
            FROM saved_files
            WHERE url = %(url)s AND file_type = %(file_type)s
            """
            results = await self.snowflake.execute_query(
                query,
                {"url": url, "file_type": file_type}
            )
            
            if not results:
                return False
            
            stage_path = results[0]["stage_path"]
            
            # Remove from stage
            remove_query = f"REMOVE {stage_path}"
            await self.snowflake.execute_query(remove_query)
            
            # Delete record
            delete_query = """
            DELETE FROM saved_files
            WHERE url = %(url)s AND file_type = %(file_type)s
            """
            await self.snowflake.execute_query(
                delete_query,
                {"url": url, "file_type": file_type}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error deleting file: {str(e)}")
            return False