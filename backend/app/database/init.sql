-- Initialize Snowflake Environment
USE ROLE ACCOUNTADMIN;

-- Create or replace warehouse
CREATE OR REPLACE WAREHOUSE COMPUTE_WH 
WAREHOUSE_SIZE = 'MEDIUM' 
AUTO_SUSPEND = 300;

-- Create database and schema
CREATE DATABASE IF NOT EXISTS LLM;
CREATE SCHEMA IF NOT EXISTS LLM.RAG;

USE LLM.RAG;

-- Create main stage
CREATE OR REPLACE STAGE crawled_content
    FILE_FORMAT = (
        TYPE = 'JSON'
        STRIP_OUTER_ARRAY = TRUE
    );

-- Create file formats for different types
CREATE OR REPLACE FILE FORMAT json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

CREATE OR REPLACE FILE FORMAT markdown_format
    TYPE = 'CSV'  -- Using CSV as base type for text
    FIELD_DELIMITER = 'NONE'
    RECORD_DELIMITER = 'NONE';

CREATE OR REPLACE FILE FORMAT binary_format
    TYPE = 'CSV'  -- Using CSV as base type for binary
    FIELD_DELIMITER = 'NONE'
    RECORD_DELIMITER = 'NONE'
    BINARY_FORMAT = HEX;

-- Create core tables
CREATE OR REPLACE TABLE discovery_results (
    task_id STRING PRIMARY KEY,
    start_url STRING,
    discovered_urls ARRAY,
    total_urls INTEGER,
    max_depth INTEGER,
    url_graph VARIANT,
    created_at TIMESTAMP_NTZ,
    completed_at TIMESTAMP_NTZ
);

CREATE OR REPLACE TABLE crawl_results (
    url STRING PRIMARY KEY,
    success BOOLEAN,
    html STRING,
    cleaned_html STRING,
    error_message STRING,
    media_data VARIANT,
    links_data VARIANT,
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE saved_files (
    url STRING,
    file_type STRING,
    stage_path STRING,
    content_type STRING,
    size NUMBER,
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (url, file_type, stage_path)
);

CREATE OR REPLACE TABLE task_metrics (
    task_id STRING,
    timestamp TIMESTAMP_NTZ,
    metrics VARIANT,
    PRIMARY KEY (task_id, timestamp)
);

-- Create python UDF for markdown processing
CREATE OR REPLACE FUNCTION py_read_markdown(file string)
    RETURNS STRING
    LANGUAGE PYTHON
    RUNTIME_VERSION = '3.8'
    PACKAGES = ('snowflake-snowpark-python', 'markdown', 'mistune')
    HANDLER = 'read_file'
AS 
'
import mistune
from snowflake.snowpark.files import SnowflakeFile
from html.parser import HTMLParser

def read_file(file_path):
    with SnowflakeFile.open(file_path, "r") as file:
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
                return " ".join(self.text)
                
        stripper = MLStripper()
        stripper.feed(html_content)
        plain_text = stripper.get_data()
        
        return plain_text
';

-- Create chunking procedure
CREATE OR REPLACE PROCEDURE CHUNK_CONTENT(chunk_size FLOAT, overlap FLOAT)
RETURNS STRING
LANGUAGE SQL
AS
$$
BEGIN
    CREATE OR REPLACE TABLE documentations_chunked AS 
    WITH RECURSIVE split_contents AS (
        SELECT 
            file_name,
            SUBSTRING(contents, 1, chunk_size::INTEGER) AS chunk_text,
            SUBSTRING(contents, (chunk_size-overlap)::INTEGER) AS remaining_contents,
            1 AS chunk_number
        FROM 
            documentations

        UNION ALL

        SELECT 
            file_name,
            SUBSTRING(remaining_contents, 1, chunk_size::INTEGER),
            SUBSTRING(remaining_contents, (chunk_size+1)::INTEGER),
            chunk_number + 1
        FROM 
            split_contents
        WHERE 
            LENGTH(remaining_contents) > 0
    )
    SELECT 
        file_name,
        chunk_number,
        chunk_text,
        CONCAT(
            'Sampled contents from documentations [', 
            file_name,
            ']: ', 
            chunk_text
        ) AS combined_chunk_text
    FROM 
        split_contents
    ORDER BY 
        file_name,
        chunk_number;
    
    RETURN 'Content chunking completed successfully';
END;
$$
;


-- Create search function
CREATE OR REPLACE FUNCTION SEARCH_DOCUMENTS(query_text STRING)
RETURNS TABLE (file_name STRING, chunk_text STRING, score FLOAT)
LANGUAGE SQL
AS
$$
    SELECT
        file_name,
        chunk_text,
        VECTOR_COSINE_SIMILARITY(
            combined_chunk_vector, 
            SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                'snowflake-arctic-embed-m-v1.5', 
                query_text
            )
        ) AS score
    FROM 
        documentations_chunked_vectors
    ORDER BY 
        score DESC
    LIMIT 5
$$
;
