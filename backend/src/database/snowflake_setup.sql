-- Use existing database and schema
USE LLM.RAG;

-- Add tables for crawler metadata while maintaining RAG compatibility
CREATE TABLE IF NOT EXISTS crawl_metadata (
    url STRING PRIMARY KEY,
    success BOOLEAN,
    error_message STRING,
    metadata OBJECT,  -- JSON object for flexible metadata
    timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    file_name STRING,  -- Links to documentations table
    file_type STRING,  -- 'markdown', 'pdf', 'image', 'screenshot'
    content_type STRING,  -- MIME type
    size NUMBER  -- File size in bytes
);

-- Add UDF to process different file types (PDFs, images)
CREATE OR REPLACE FUNCTION process_binary_file(file_content BINARY, file_type STRING)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'python-magic')
    HANDLER = 'process_file'
AS 
$$
import magic

def process_file(file_content, file_type):
    # Basic file validation and metadata extraction
    mime = magic.Magic(mime=True)
    detected_type = mime.from_buffer(file_content)
    
    metadata = {
        'mime_type': detected_type,
        'size': len(file_content)
    }
    
    return str(metadata)
$$;

-- Add procedure to sync documentations with crawl_metadata
CREATE OR REPLACE PROCEDURE sync_crawl_content()
    RETURNS STRING
    LANGUAGE SQL
AS
BEGIN
    MERGE INTO documentations d
    USING crawl_metadata cm
    ON d.file_name = cm.file_name
    WHEN NOT MATCHED AND cm.file_type = 'markdown' THEN
        INSERT (file_name, contents)
        VALUES (cm.file_name, cm.metadata:markdown_content::STRING);
    
    RETURN 'Sync completed';
END;