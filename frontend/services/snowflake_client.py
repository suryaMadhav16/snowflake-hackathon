import logging
from typing import Optional, Dict, List
from datetime import datetime
from snowflake.snowpark import Session
import streamlit as st

logger = logging.getLogger(__name__)

class SnowflakeClient:
    """Singleton client for managing Snowflake connections"""
    _instance = None
    _session = None
    _initialized = False

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SnowflakeClient, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Only initialize once
        if not SnowflakeClient._initialized:
            self._initialize_connection()
            SnowflakeClient._initialized = True

    def _initialize_connection(self):
        """Initialize the shared Snowflake connection"""
        if SnowflakeClient._session is None:
            logger.info("Initializing Snowflake connection...")
            try:
                session_parameters = {
                    "account": st.secrets["snowflake"]["account"],
                    "user": st.secrets["snowflake"]["user"],
                    "warehouse": st.secrets["snowflake"]["warehouse"],
                    "database": st.secrets["snowflake"]["database"],
                    "schema": st.secrets["snowflake"]["schema"]
                }

                # Handle different authentication methods
                if "password" in st.secrets["snowflake"]:
                    session_parameters["password"] = st.secrets["snowflake"]["password"]
                elif "private_key" in st.secrets["snowflake"]:
                    session_parameters["private_key"] = st.secrets["snowflake"]["private_key"]
                elif "authenticator" in st.secrets["snowflake"]:
                    session_parameters["authenticator"] = st.secrets["snowflake"]["authenticator"]

                SnowflakeClient._session = Session.builder.configs(session_parameters).create()
                self._ensure_tables_exist()
                logger.info("Successfully initialized Snowflake connection")

            except Exception as e:
                logger.error(f"Failed to initialize Snowflake connection: {str(e)}", exc_info=True)
                SnowflakeClient._session = None
                raise

    @property
    def session(self) -> Optional[Session]:
        """Get the shared Snowflake session"""
        if not SnowflakeClient._session:
            self._initialize_connection()
        return SnowflakeClient._session

    def _ensure_tables_exist(self):
        """Ensure required tables exist in Snowflake"""
        try:
            tables = [
                """CREATE TABLE IF NOT EXISTS DOCUMENTATIONS (
                    FILE_NAME TEXT NOT NULL PRIMARY KEY,
                    CONTENTS TEXT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )""",
                """CREATE TABLE IF NOT EXISTS DOCUMENTATIONS_CHUNKED (
                    FILE_NAME TEXT NOT NULL,
                    CHUNK_NUMBER NUMBER NOT NULL,
                    CHUNK_TEXT TEXT,
                    COMBINED_CHUNK_TEXT TEXT,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
                )""",
                """CREATE TABLE IF NOT EXISTS DOCUMENTATIONS_CHUNKED_VECTORS (
                    FILE_NAME TEXT NOT NULL,
                    CHUNK_NUMBER NUMBER NOT NULL,
                    COMBINED_CHUNK_TEXT TEXT,
                    COMBINED_CHUNK_VECTOR VECTOR(FLOAT, 768),
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
                )"""
            ]

            for table_sql in tables:
                self.session.sql(table_sql).collect()

            logger.info("Successfully ensured all required tables exist")

        except Exception as e:
            logger.error(f"Error ensuring tables exist: {str(e)}", exc_info=True)
            raise

    def insert_document(self, file_name: str, contents: str) -> bool:
        """Insert a document into Snowflake"""
        try:
            self.session.sql("""
                INSERT INTO DOCUMENTATIONS (FILE_NAME, CONTENTS)
                VALUES (?, ?)
            """, params=[file_name, contents]).collect()
            return True
        except Exception as e:
            logger.error(f"Failed to insert document {file_name}: {str(e)}", exc_info=True)
            return False

    def create_chunks(self, file_name: str, chunk_size: int = 512, overlap: int = 50) -> bool:
        """Create chunks from a document"""
        try:
            self.session.sql("""
                INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (
                    FILE_NAME,
                    CHUNK_NUMBER,
                    COMBINED_CHUNK_TEXT
                )
                SELECT 
                    d.FILE_NAME,
                    ROW_NUMBER() OVER (PARTITION BY d.FILE_NAME ORDER BY chunk.seq),
                    CONCAT('Content from page [', d.FILE_NAME, ']: ', chunk.value::TEXT)
                FROM DOCUMENTATIONS d,
                     LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                         d.CONTENTS,
                         'markdown',
                         ?, ?
                     )) chunk
                WHERE d.FILE_NAME = ?
            """, params=[chunk_size, overlap, file_name]).collect()
            return True
        except Exception as e:
            logger.error(f"Failed to create chunks for {file_name}: {str(e)}", exc_info=True)
            return False
    
    def sync_cral_content(self):
        self.session.sql("CALL LLM.RAG.SYNC_CRAWL_CONTENT()").collect()
        
    def generate_embeddings(self, file_name: str, model: str = 'snowflake-arctic-embed-m-v1.5') -> bool:
        """Generate embeddings for document chunks"""
        try:
            self.session.sql("""
                UPDATE DOCUMENTATIONS_CHUNKED_VECTORS
                SET COMBINED_CHUNK_VECTOR = SNOWFLAKE.CORTEX.EMBED_TEXT_768(?, COMBINED_CHUNK_TEXT)::VECTOR(FLOAT, 768)
                WHERE FILE_NAME = ?
            """, params=[model, file_name]).collect()
            return True
        except Exception as e:
            logger.error(f"Failed to generate embeddings for {file_name}: {str(e)}", exc_info=True)
            return False

    def similar_chunks(self, query: str, num_chunks: int = 3, similarity_threshold: float = 0.7) -> List[Dict]:
        """Find similar chunks for a given query"""
        try:
            result = self.session.sql("""
                WITH embedded_question AS (
                    SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', ?) AS question_vector
                )
                SELECT
                    file_name,
                    chunk_number,
                    combined_chunk_text AS chunk_text,
                    VECTOR_COSINE_SIMILARITY(
                        combined_chunk_vector, 
                        (SELECT question_vector FROM embedded_question)
                    ) AS similarity
                FROM 
                    documentations_chunked_vectors
                WHERE 
                    VECTOR_COSINE_SIMILARITY(
                        combined_chunk_vector,
                        (SELECT question_vector FROM embedded_question)
                    ) >= ?
                ORDER BY 
                    similarity DESC
                LIMIT ?
            """, params=[query, similarity_threshold, num_chunks]).collect()
            return [dict(row) for row in result]
        except Exception as e:
            logger.error(f"Failed to find similar chunks: {str(e)}", exc_info=True)
            return []

    def generate_response(self, question: str, context: Optional[str] = None) -> str:
        """Generate a response using Snowflake Cortex"""
        try:
            if context:
                prompt = f"""You are an AI assistant helping with documentation questions.
                Use the following context to answer the question:
                
                Context:
                {context}
                
                Question: {question}
                
                Answer the question based only on the provided context. If the context doesn't contain relevant information, say so."""
            else:
                prompt = f"""You are an AI assistant.
                Question: {question}
                
                Please provide a helpful response based on general knowledge since no specific documentation context was found."""

            result = self.session.sql("""
                SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', 
                    CONCAT('Answer this question: ', ?, 
                          CASE WHEN ? IS NOT NULL THEN CONCAT('\n\nUsing this context: ', ?) ELSE '' END)
                ) AS response
            """, params=[question, context, context]).collect()

            return result[0]["RESPONSE"]
        except Exception as e:
            logger.error(f"Failed to generate response: {str(e)}", exc_info=True)
            return "I encountered an error while generating the response."

    @classmethod
    def close_connection(cls):
        """Close the shared Snowflake connection"""
        if cls._session:
            try:
                cls._session.close()
                cls._session = None
                cls._initialized = False
                logger.info("Snowflake connection closed")
            except Exception as e:
                logger.error(f"Error closing Snowflake connection: {str(e)}", exc_info=True)
