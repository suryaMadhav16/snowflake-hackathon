import streamlit as st
import snowflake.snowpark as snowpark
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)

class SnowflakeManager:
    """Manages Snowflake operations for the frontend"""
    
    def __init__(self):
        """Initialize Snowflake manager with connection parameters"""
        self.connection_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "password": st.secrets["snowflake"]["password"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"],
            "role": st.secrets["snowflake"]["role"]
        }
        self._session = None
    
    @property
    def session(self) -> snowpark.Session:
        """Get or create Snowflake session"""
        if not self._session:
            try:
                self._session = snowpark.Session.builder.configs(
                    self.connection_parameters
                ).create()
                logger.info("Connected to Snowflake successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Snowflake: {str(e)}")
                raise
        return self._session
    
    def process_document(
        self,
        file_path: str,
        metadata: Optional[Dict] = None
    ) -> bool:
        """Process uploaded document"""
        try:
            # Upload to stage
            self.session.file.put(
                file_path,
                "@documentations",
                auto_compress=False
            )
            
            # Process through markdown reader
            self.session.sql(f"""
                INSERT INTO documentations 
                SELECT 
                    '{file_path}' as file_name,
                    py_read_markdown(
                        build_scoped_file_url(@documentations, '{file_path}')
                    ) AS contents,
                    PARSE_JSON(%s) as metadata
            """, params=[metadata] if metadata else [None]).collect()
            
            return True
        except Exception as e:
            logger.error(f"Error processing document: {str(e)}")
            return False
    
    def search_documents(
        self,
        query: str,
        limit: int = 10
    ) -> List[Dict]:
        """Search through processed documents"""
        try:
            results = self.session.sql(f"""
                SELECT 
                    file_name,
                    contents,
                    metadata,
                    SIMILARITY_SCORE(contents, '{query}') as score
                FROM documentations
                WHERE CONTAINS(contents, '{query}')
                ORDER BY score DESC
                LIMIT {limit}
            """).collect()
            
            return [row.as_dict() for row in results]
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}")
            return []
    
    def get_document_content(self, file_name: str) -> Optional[Dict]:
        """Get document content by file name"""
        try:
            result = self.session.sql("""
                SELECT file_name, contents, metadata
                FROM documentations
                WHERE file_name = %s
                LIMIT 1
            """, params=[file_name]).collect()
            
            if result:
                return result[0].as_dict()
            return None
        except Exception as e:
            logger.error(f"Error getting document content: {str(e)}")
            return None
    
    def list_documents(self) -> List[Dict]:
        """List all processed documents"""
        try:
            results = self.session.sql("""
                SELECT file_name, metadata
                FROM documentations
                ORDER BY file_name
            """).collect()
            
            return [row.as_dict() for row in results]
        except Exception as e:
            logger.error(f"Error listing documents: {str(e)}")
            return []
    
    def delete_document(self, file_name: str) -> bool:
        """Delete a document from Snowflake"""
        try:
            self.session.sql("""
                DELETE FROM documentations
                WHERE file_name = %s
            """, params=[file_name]).collect()
            
            # Also remove from stage
            self.session.sql(f"""
                REMOVE @documentations/{file_name}
            """).collect()
            
            return True
        except Exception as e:
            logger.error(f"Error deleting document: {str(e)}")
            return False
    
    def close(self):
        """Close Snowflake session"""
        if self._session:
            try:
                self._session.close()
                self._session = None
            except Exception as e:
                logger.error(f"Error closing Snowflake session: {str(e)}")
