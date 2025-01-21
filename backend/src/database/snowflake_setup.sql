-- Set up environment
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE MEDIUM WAREHOUSE_SIZE='MEDIUM' AUTO_SUSPEND=300;
CREATE DATABASE IF NOT EXISTS LLM;
CREATE SCHEMA IF NOT EXISTS LLM.RAG;

USE LLM.RAG;

-- Create stage for storing files
CREATE OR REPLACE STAGE DOCUMENTATIONS
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );

-- Create tables for RAG
CREATE OR REPLACE TABLE DOCUMENTATIONS (
    FILE_NAME STRING PRIMARY KEY,
    CONTENTS STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create chunked table for RAG processing
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED (
    FILE_NAME STRING,
    CHUNK_NUMBER INTEGER,
    CHUNK_TEXT STRING,
    COMBINED_CHUNK_TEXT STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Create vectors table for embeddings
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
    FILE_NAME STRING,
    CHUNK_NUMBER INTEGER,
    CHUNK_TEXT STRING,
    COMBINED_CHUNK_TEXT STRING,
    COMBINED_CHUNK_VECTOR ARRAY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Create tables for crawler operations
CREATE OR REPLACE TABLE CRAWL_METADATA (
    URL STRING PRIMARY KEY,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE STRING,
    METADATA OBJECT,  -- JSON object for flexible metadata
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME STRING,  -- Links to DOCUMENTATIONS table
    FILE_TYPE STRING,  -- 'markdown', 'pdf', 'image', 'screenshot'
    CONTENT_TYPE STRING,  -- MIME type
    SIZE NUMBER  -- File size in bytes
);

-- Create task metrics table
CREATE OR REPLACE TABLE TASK_METRICS (
    TASK_ID STRING NOT NULL,
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    METRICS OBJECT,   -- JSON object for metrics
    PRIMARY KEY (TASK_ID, TIMESTAMP)
);

-- Create tasks table
CREATE OR REPLACE TABLE TASKS (
    TASK_ID STRING PRIMARY KEY,
    TASK_TYPE STRING NOT NULL,
    STATUS STRING NOT NULL,
    PROGRESS FLOAT DEFAULT 0.0,
    SETTINGS OBJECT,
    METRICS OBJECT,
    ERROR STRING,
    CURRENT_URL STRING,
    URLS ARRAY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT TIMESTAMP_NTZ
);

-- Create discovery results table
CREATE OR REPLACE TABLE DISCOVERY_RESULTS (
    TASK_ID STRING PRIMARY KEY,
    START_URL STRING,
    DISCOVERED_URLS ARRAY,
    TOTAL_URLS NUMBER,
    MAX_DEPTH NUMBER,
    URL_GRAPH OBJECT,
    CREATED_AT TIMESTAMP_NTZ,
    COMPLETED_AT TIMESTAMP_NTZ
);

-- Create UDF for markdown processing
CREATE OR REPLACE FUNCTION PY_READ_MARKDOWN(file STRING)
    RETURNS STRING 
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'markdown', 'mistune')
    HANDLER = 'read_file'
AS 
$$
import mistune
from snowflake.snowpark.files import SnowflakeFile
from html.parser import HTMLParser

def read_file(file_path):
    with SnowflakeFile.open(file_path, 'r') as file:
        markdown_content = file.read()
        html_content = mistune.html(markdown_content)
        
        class MLStripper(HTMLParser):
            def __init__(self):
                super().__init__()
                self.reset()
                self.strict = False
                self.convert_charrefs = True
                self.text = []
                
            def handle_data(self, d):
                self.text.append(d)
                
            def get_data(self):
                return ' '.join(self.text)
                
        stripper = MLStripper()
        stripper.feed(html_content)
        plain_text = stripper.get_data()
        
        return plain_text
$$;

-- Create UDF for binary file processing
CREATE OR REPLACE FUNCTION PROCESS_BINARY_FILE(file_content BINARY, file_type STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'python-magic')
    HANDLER = 'process_file'
AS 
$$
import magic

def process_file(file_content, file_type):
    try:
        mime = magic.Magic(mime=True)
        detected_type = mime.from_buffer(file_content)
        
        metadata = {
            'MIME_TYPE': detected_type,
            'SIZE': len(file_content),
            'FILE_TYPE': file_type
        }
        
        return str(metadata)
    except Exception as e:
        return str({'ERROR': str(e)})
$$;

-- Create procedure for syncing crawl content
CREATE OR REPLACE PROCEDURE SYNC_CRAWL_CONTENT()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    DECLARE
        sync_count NUMBER DEFAULT 0;
        error_message STRING DEFAULT NULL;
    BEGIN
        -- Begin transaction
        BEGIN TRANSACTION;
        
        -- Sync markdown content to DOCUMENTATIONS table
        MERGE INTO DOCUMENTATIONS d
        USING (
            SELECT 
                FILE_NAME,
                METADATA:MARKDOWN_CONTENT::STRING as CONTENTS
            FROM CRAWL_METADATA
            WHERE FILE_TYPE = 'markdown'
            AND METADATA:MARKDOWN_CONTENT IS NOT NULL
        ) cm
        ON d.FILE_NAME = cm.FILE_NAME
        WHEN NOT MATCHED THEN
            INSERT (FILE_NAME, CONTENTS)
            VALUES (cm.FILE_NAME, cm.CONTENTS)
        WHEN MATCHED THEN
            UPDATE SET CONTENTS = cm.CONTENTS;
            
        SET sync_count = number_of_rows_inserted + number_of_rows_updated;
        
        -- Commit transaction
        COMMIT;
        
        RETURN 'Sync completed successfully: ' || sync_count || ' rows processed';
        
    EXCEPTION
        WHEN OTHER THEN
            SET error_message = 'Error during sync: ' || SQLSTATE || ': ' || SQLERRM;
            ROLLBACK;
            RETURN error_message;
    END;
END;