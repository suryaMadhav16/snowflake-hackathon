----------------------------------------------------------------------
-- 1. Create Database and Schema for RAG Chat Application
----------------------------------------------------------------------
CREATE OR REPLACE DATABASE RAG_CHAT_DB;
CREATE OR REPLACE SCHEMA RAG_CHAT_SCHEMA;
USE DATABASE RAG_CHAT_DB;
USE SCHEMA RAG_CHAT_SCHEMA;

----------------------------------------------------------------------
-- 2. Create Stage for Document Upload (with directory table)
----------------------------------------------------------------------
CREATE OR REPLACE STAGE document_stage
  ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
  DIRECTORY = (ENABLE = TRUE);

----------------------------------------------------------------------
-- 3. Create Python UDTF for Document Text Chunking
----------------------------------------------------------------------
CREATE OR REPLACE FUNCTION text_chunker(doc_text STRING)
  RETURNS TABLE (chunk VARCHAR)
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.9'
  HANDLER = 'text_chunker'
  PACKAGES = ('snowflake-snowpark-python', 'langchain')
AS
$$
from snowflake.snowpark.types import StringType
from langchain.text_splitter import RecursiveCharacterTextSplitter
import pandas as pd
import traceback

class text_chunker:
    def process(self, doc_text: str):
        """Process document text into chunks.
        
        Args:
            doc_text: Input text to be chunked
            
        Yields:
            Tuple containing a single chunk of text
            
        Note:
            If an error occurs, yields an error message as a chunk
        """
        try:
            # Input validation
            if not doc_text or not isinstance(doc_text, str):
                raise ValueError("Input must be a non-empty string")

            # Create text splitter with specific parameters
            text_splitter = RecursiveCharacterTextSplitter(
                chunk_size=1000,
                chunk_overlap=200,
                length_function=len,
                separators=["\n\n", "\n", " ", ""]
            )
            
            # Split text into chunks
            chunks = text_splitter.split_text(doc_text)
            
            # Validate chunks
            if not chunks:
                raise ValueError("Text splitting produced no chunks")

            # Create DataFrame with 'chunk' column (matching UDTF return type)
            df = pd.DataFrame(chunks, columns=['chunk'])
            
            # Yield each chunk
            yield from df.itertuples(index=False, name=None)

        except Exception as e:
            # Get full error traceback
            error_msg = f"Error in text_chunker: {str(e)}\n{traceback.format_exc()}"
            print(error_msg)  # This will be logged in Snowflake's query history
            
            # Yield error message as a chunk
            yield (f"Error processing document: {str(e)}",)
$$;

----------------------------------------------------------------------
-- 4. Create Table for Document Chunks
----------------------------------------------------------------------
CREATE OR REPLACE TABLE DOCUMENT_CHUNKS (
    RELATIVE_PATH     VARCHAR(512),       -- Limited for path length
    FILE_NAME        VARCHAR(256),        -- Limited for filename length
    CHUNK_ID         NUMBER AUTOINCREMENT,
    CHUNK_TEXT       VARCHAR(16777216),   -- Maximum VARCHAR size
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_TYPE        VARCHAR(10),         -- Limited to PDF or MD
    FILE_SIZE        NUMBER,
    METADATA         VARIANT              -- For any additional metadata
);

----------------------------------------------------------------------
-- 5. Create Cortex Search Service
----------------------------------------------------------------------
CREATE OR REPLACE CORTEX SEARCH SERVICE RAG_SEARCH_SERVICE
  ON CHUNK_TEXT
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 minute'
AS (
  SELECT CHUNK_TEXT,
         RELATIVE_PATH,
         FILE_NAME,
         CHUNK_ID,
         FILE_TYPE,
         METADATA
  FROM DOCUMENT_CHUNKS
);

----------------------------------------------------------------------
-- 6. Create Table for Chat History (Optional)
----------------------------------------------------------------------
CREATE OR REPLACE TABLE CHAT_HISTORY (
    CHAT_ID          VARCHAR(36),         -- UUID length
    MESSAGE_ID       NUMBER AUTOINCREMENT,
    ROLE             VARCHAR(10),         -- user or assistant
    CONTENT          VARCHAR(16777216),   -- Maximum VARCHAR size
    CREATED_AT       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    METADATA         VARIANT              -- For storing retrieved chunks, scores etc.
);

----------------------------------------------------------------------
-- 7. Verify Setup
----------------------------------------------------------------------
SHOW TABLES;
SHOW USER FUNCTIONS;
SHOW STAGES;
SHOW CORTEX SEARCH SERVICES;