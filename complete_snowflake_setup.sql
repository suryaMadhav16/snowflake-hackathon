-- Initial Setup
USE ROLE ACCOUNTADMIN;

-- Create warehouse
CREATE OR REPLACE WAREHOUSE MEDIUM_WAREHOUSE 
    WAREHOUSE_SIZE = 'MEDIUM' 
    AUTO_SUSPEND = 300 
    AUTO_RESUME = TRUE;

-- Create database and schema
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA LLM.RAG;

-- Set context
USE WAREHOUSE MEDIUM_WAREHOUSE;
USE DATABASE LLM;
USE SCHEMA RAG;

-- Create stage for storing files
CREATE OR REPLACE STAGE DOCUMENTATIONS
    DIRECTORY = (
        ENABLE = TRUE
    )
    FILE_FORMAT = (
        TYPE = 'CSV'
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    );

-- Create tables
-- Main documentations table
CREATE OR REPLACE TABLE DOCUMENTATIONS (
    FILE_NAME TEXT NOT NULL PRIMARY KEY,
    CONTENTS TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Chunked content table
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED (
    FILE_NAME TEXT NOT NULL,
    CHUNK_NUMBER NUMBER NOT NULL,
    CHUNK_TEXT TEXT,
    COMBINED_CHUNK_TEXT TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Vector embeddings table
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
    FILE_NAME TEXT NOT NULL,
    CHUNK_NUMBER NUMBER NOT NULL,
    CHUNK_TEXT TEXT,
    COMBINED_CHUNK_TEXT TEXT,
    COMBINED_CHUNK_VECTOR ARRAY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Crawler metadata table
CREATE OR REPLACE TABLE CRAWL_METADATA (
    URL TEXT NOT NULL PRIMARY KEY,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE TEXT,
    METADATA VARIANT,
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME TEXT,
    FILE_TYPE TEXT,
    CONTENT_TYPE TEXT,
    SIZE NUMBER
);

-- Task metrics table
CREATE OR REPLACE TABLE TASK_METRICS (
    TASK_ID TEXT NOT NULL,
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    METRICS VARIANT,
    PRIMARY KEY (TASK_ID, TIMESTAMP)
);

-- Tasks table
CREATE OR REPLACE TABLE TASKS (
    TASK_ID TEXT PRIMARY KEY,
    TASK_TYPE TEXT NOT NULL,
    STATUS TEXT NOT NULL,
    PROGRESS FLOAT DEFAULT 0.0,
    SETTINGS VARIANT,
    METRICS VARIANT,
    ERROR TEXT,
    CURRENT_URL TEXT,
    URLS ARRAY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    COMPLETED_AT TIMESTAMP_NTZ
);

-- Discovery results table
CREATE OR REPLACE TABLE DISCOVERY_RESULTS (
    TASK_ID TEXT PRIMARY KEY,
    START_URL TEXT,
    DISCOVERED_URLS ARRAY,
    TOTAL_URLS NUMBER,
    MAX_DEPTH NUMBER,
    URL_GRAPH VARIANT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
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

-- Create procedure for syncing content
CREATE OR REPLACE PROCEDURE SYNC_CRAWL_CONTENT()
    RETURNS VARIANT
    LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    rows_inserted INTEGER DEFAULT 0;
BEGIN
    -- Step 1: Merge markdown content to DOCUMENTATIONS table
    MERGE INTO DOCUMENTATIONS d
    USING (
        SELECT 
            FILE_NAME,
            METADATA:markdown_content::STRING as CONTENTS
        FROM CRAWL_METADATA
        WHERE FILE_TYPE = 'markdown'
        AND METADATA:markdown_content IS NOT NULL
    ) m
    ON d.FILE_NAME = m.FILE_NAME
    WHEN NOT MATCHED THEN
        INSERT (FILE_NAME, CONTENTS)
        VALUES (m.FILE_NAME, m.CONTENTS)
    WHEN MATCHED THEN
        UPDATE SET CONTENTS = m.CONTENTS;
    
    SET rows_inserted = SQLROWCOUNT;

    -- Step 2: Create chunks for new documents
    INSERT INTO DOCUMENTATIONS_CHUNKED (
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT
    )
    WITH document_lengths AS (
        SELECT 
            FILE_NAME,
            CONTENTS,
            CEIL(LENGTH(CONTENTS)::FLOAT / 3000) as num_chunks
        FROM DOCUMENTATIONS d
        WHERE NOT EXISTS (
            SELECT 1 FROM DOCUMENTATIONS_CHUNKED dc
            WHERE dc.FILE_NAME = d.FILE_NAME
        )
    ),
    chunk_ranges AS (
        SELECT 
            FILE_NAME,
            CONTENTS,
            SEQ4() + 1 as CHUNK_NUMBER,
            1 + (SEQ4() * 3000) as chunk_start
        FROM document_lengths
        WHERE SEQ4() < num_chunks
    )
    SELECT 
        cr.FILE_NAME,
        cr.CHUNK_NUMBER,
        SUBSTRING(cr.CONTENTS, cr.chunk_start, 3000) as CHUNK_TEXT,
        CONCAT(
            'Content from document [', 
            cr.FILE_NAME, 
            ']: ',
            SUBSTRING(cr.CONTENTS, cr.chunk_start, 3000)
        ) as COMBINED_CHUNK_TEXT
    FROM chunk_ranges cr;

    -- Step 3: Generate embeddings for new chunks
    INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT,
        COMBINED_CHUNK_VECTOR
    )
    SELECT 
        dc.FILE_NAME,
        dc.CHUNK_NUMBER,
        dc.CHUNK_TEXT,
        dc.COMBINED_CHUNK_TEXT,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            dc.COMBINED_CHUNK_TEXT
        ) as COMBINED_CHUNK_VECTOR
    FROM DOCUMENTATIONS_CHUNKED dc
    LEFT JOIN DOCUMENTATIONS_CHUNKED_VECTORS dcv
        ON dc.FILE_NAME = dcv.FILE_NAME
        AND dc.CHUNK_NUMBER = dcv.CHUNK_NUMBER
    WHERE dcv.FILE_NAME IS NULL;

    -- Return success result
    SELECT OBJECT_CONSTRUCT(
        'status', 'success',
        'message', 'Content sync completed successfully',
        'rows_processed', rows_inserted
    ) INTO :result;

    RETURN result;

EXCEPTION
    WHEN OTHER THEN
        SELECT OBJECT_CONSTRUCT(
            'status', 'error',
            'message', 'Content sync failed: ' || SQLERRM,
            'error_code', SQLCODE,
            'error_state', SQLSTATE
        ) INTO :result;
        RETURN result;
END;
$$;

-- Create function to answer queries using RAG
CREATE OR REPLACE FUNCTION ANSWER_QUERY(query_text STRING)
RETURNS TABLE (response STRING, file_name STRING, chunk_text STRING, chunk_number NUMBER, similarity FLOAT)
AS
$$
WITH embedded_query AS (
    SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', query_text) as query_vector
),
similar_chunks AS (
    SELECT
        dcv.FILE_NAME,
        dcv.CHUNK_NUMBER,
        dcv.CHUNK_TEXT,
        VECTOR_COSINE_SIMILARITY(
            dcv.COMBINED_CHUNK_VECTOR,
            (SELECT query_vector FROM embedded_query)
        ) as similarity
    FROM DOCUMENTATIONS_CHUNKED_VECTORS dcv
    QUALIFY ROW_NUMBER() OVER (ORDER BY similarity DESC) <= 5
    ORDER BY similarity DESC
)
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        CONCAT(
            'Answer this question using the provided context. If the context does not contain relevant information, say so.\n\n',
            'Question: ', query_text, '\n\n',
            'Context:\n', LISTAGG(CHUNK_TEXT, '\n\n') WITHIN GROUP (ORDER BY similarity DESC)
        )
    ) as response,
    FILE_NAME,
    CHUNK_TEXT,
    CHUNK_NUMBER,
    similarity
FROM similar_chunks
$$;

-- Create procedure to chunk documents
CREATE OR REPLACE PROCEDURE CHUNK_DOCUMENTS(
    chunk_size NUMBER DEFAULT 3000,
    overlap_size NUMBER DEFAULT 1000
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
    chunks_created INTEGER DEFAULT 0;
BEGIN
    -- Clear existing chunks
    DELETE FROM DOCUMENTATIONS_CHUNKED_VECTORS;
    DELETE FROM DOCUMENTATIONS_CHUNKED;
    
    -- Create new chunks
    INSERT INTO DOCUMENTATIONS_CHUNKED (
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT
    )
    WITH document_lengths AS (
        SELECT 
            FILE_NAME,
            CONTENTS,
            CEIL(LENGTH(CONTENTS)::FLOAT / (chunk_size - overlap_size)) as num_chunks
        FROM DOCUMENTATIONS
    ),
    chunk_ranges AS (
        SELECT 
            FILE_NAME,
            CONTENTS,
            SEQ4() + 1 as CHUNK_NUMBER,
            1 + (SEQ4() * (chunk_size - overlap_size)) as chunk_start
        FROM document_lengths
        WHERE SEQ4() < num_chunks
    )
    SELECT 
        cr.FILE_NAME,
        cr.CHUNK_NUMBER,
        SUBSTRING(cr.CONTENTS, cr.chunk_start, chunk_size) as CHUNK_TEXT,
        CONCAT(
            'Content from document [', 
            cr.FILE_NAME, 
            ']: ',
            SUBSTRING(cr.CONTENTS, cr.chunk_start, chunk_size)
        ) as COMBINED_CHUNK_TEXT
    FROM chunk_ranges cr;

    SET chunks_created = SQLROWCOUNT;

    -- Generate embeddings for all chunks
    INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT,
        COMBINED_CHUNK_VECTOR
    )
    SELECT 
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            COMBINED_CHUNK_TEXT
        ) as COMBINED_CHUNK_VECTOR
    FROM DOCUMENTATIONS_CHUNKED;

    -- Return result
    SELECT OBJECT_CONSTRUCT(
        'status', 'success',
        'chunks_created', chunks_created,
        'message', 'Successfully created and vectorized chunks'
    ) INTO :result;

    RETURN result;

EXCEPTION
    WHEN OTHER THEN
        SELECT OBJECT_CONSTRUCT(
            'status', 'error',
            'chunks_created', chunks_created,
            'message', 'Error creating chunks: ' || SQLERRM,
            'error_code', SQLCODE,
            'error_state', SQLSTATE
        ) INTO :result;
        RETURN result;
END;
$$;

-- Create stored procedure to clean up old data
CREATE OR REPLACE PROCEDURE CLEANUP_OLD_DATA(days_to_keep NUMBER)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
BEGIN
    -- Delete old data from all tables
    DELETE FROM DOCUMENTATIONS_CHUNKED_VECTORS
    WHERE CREATED_AT < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM DOCUMENTATIONS_CHUNKED
    WHERE CREATED_AT < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM DOCUMENTATIONS
    WHERE CREATED_AT < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM CRAWL_METADATA
    WHERE TIMESTAMP < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM TASK_METRICS
    WHERE TIMESTAMP < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM TASKS
    WHERE CREATED_AT < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());
    
    DELETE FROM DISCOVERY_RESULTS
    WHERE CREATED_AT < DATEADD(day, -days_to_keep, CURRENT_TIMESTAMP());

    -- Return result
    SELECT OBJECT_CONSTRUCT(
        'status', 'success',
        'message', 'Successfully cleaned up data older than ' || days_to_keep || ' days'
    ) INTO :result;

    RETURN result;

EXCEPTION
    WHEN OTHER THEN
        SELECT OBJECT_CONSTRUCT(
            'status', 'error',
            'message', 'Error cleaning up data: ' || SQLERRM,
            'error_code', SQLCODE,
            'error_state', SQLSTATE
        ) INTO :result;
        RETURN result;
END;
$$;

-- Grant necessary privileges
GRANT USAGE ON WAREHOUSE MEDIUM_WAREHOUSE TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE LLM TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL SCHEMAS IN DATABASE LLM TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL TABLES IN SCHEMA LLM.RAG TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA LLM.RAG TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL PROCEDURES IN SCHEMA LLM.RAG TO ROLE ACCOUNTADMIN;
GRANT ALL ON ALL STAGES IN SCHEMA LLM.RAG TO ROLE ACCOUNTADMIN;

-- Verify setup
SELECT 'Setup completed successfully' as status;