-- Warehouse setup with vector processing optimizations
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE MEDIUM 
    WAREHOUSE_SIZE = 'LARGE'  -- For better vector processing
    AUTO_SUSPEND = 600;       -- Extended timeout

-- Database and schema setup (original names preserved)
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA LLM.RAG;
USE SCHEMA LLM.RAG;

-- Markdown-optimized stage configuration (TYPE=TEXT)
CREATE OR REPLACE STAGE DOCUMENTATIONS
    DIRECTORY = (ENABLE = TRUE)
    FILE_FORMAT = (
        TYPE = CSV  -- CSV is required for text processing
        FIELD_DELIMITER = NONE  -- Treat entire content as single field
        SKIP_HEADER = 0  -- No headers in markdown files
        ESCAPE_UNENCLOSED_FIELD = NONE
        FIELD_OPTIONALLY_ENCLOSED_BY = NONE
    );

-- Original tables with vector type corrections
CREATE OR REPLACE TABLE DOCUMENTATIONS (
    FILE_NAME TEXT NOT NULL PRIMARY KEY,
    CONTENTS TEXT,                 -- Raw markdown storage
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
    FILE_NAME TEXT NOT NULL,
    CHUNK_NUMBER NUMBER NOT NULL,
    COMBINED_CHUNK_TEXT TEXT,
    COMBINED_CHUNK_VECTOR VECTOR(FLOAT, 768),
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (
    FILE_NAME,
    CHUNK_NUMBER,
    COMBINED_CHUNK_TEXT,
    COMBINED_CHUNK_VECTOR
)
SELECT 
    d.FILE_NAME,
    ROW_NUMBER() OVER (PARTITION BY d.FILE_NAME ORDER BY chunk.seq) AS CHUNK_NUMBER,
    chunk.value::TEXT AS COMBINED_CHUNK_TEXT,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m-v1.5',
        chunk.value::TEXT
    )::VECTOR(FLOAT, 768) AS COMBINED_CHUNK_VECTOR
FROM DOCUMENTATIONS d
, LATERAL FLATTEN(
    input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
        d.CONTENTS,
        'markdown',   -- format (none, markdown, etc.)
        512,      -- chunk_size (integer)
        50        -- overlap (integer)
    )
) chunk;


-- Modified PY_READ_MARKDOWN to preserve raw content
CREATE OR REPLACE FUNCTION PY_READ_MARKDOWN(file STRING)
RETURNS STRING 
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
HANDLER = 'read_file'
PACKAGES = ('snowflake-snowpark-python')
AS 
$$
def read_file(file_path):
    from snowflake.snowpark.files import SnowflakeFile
    with SnowflakeFile.open(file_path, 'r') as f:
        return f.read()
$$
;

-- Enhanced sync procedure with proper vector typing
CREATE OR REPLACE PROCEDURE SYNC_CRAWL_CONTENT()
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    result VARIANT;
BEGIN
    -- Merge raw markdown content with explicit aliasing
    MERGE INTO DOCUMENTATIONS d
    USING (
        SELECT 
            dir.RELATIVE_PATH AS FILE_NAME,  -- Use correct directory column
            PY_READ_MARKDOWN(
                BUILD_SCOPED_FILE_URL(@DOCUMENTATIONS, dir.RELATIVE_PATH)  -- Use RELATIVE_PATH
            ) AS CONTENTS
        FROM DIRECTORY(@DOCUMENTATIONS) dir  -- Add alias for directory table
        WHERE ENDSWITH(dir.RELATIVE_PATH, '.md')
    ) m
    ON d.FILE_NAME = m.FILE_NAME
    WHEN NOT MATCHED THEN 
        INSERT (FILE_NAME, CONTENTS)
        VALUES (m.FILE_NAME, m.CONTENTS);

    -- Chunking with explicit column references
    INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (
        FILE_NAME,
        CHUNK_NUMBER,
        COMBINED_CHUNK_TEXT,
        COMBINED_CHUNK_VECTOR
    )
    SELECT
        d.FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY d.FILE_NAME ORDER BY seq) AS CHUNK_NUMBER,
        chunk.value AS COMBINED_CHUNK_TEXT,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            chunk.value
        )::VECTOR(FLOAT, 768) AS COMBINED_CHUNK_VECTOR
    FROM DOCUMENTATIONS d,
    LATERAL SPLIT_TO_TABLE(d.CONTENTS, '\n\n') chunk;

    RETURN OBJECT_CONSTRUCT('status', 'success');

EXCEPTION
    WHEN OTHER THEN
        RETURN OBJECT_CONSTRUCT('error', SQLERRM);
END;
$$
;





CREATE OR REPLACE FUNCTION ANSWER_QUERY(query_text STRING)
RETURNS TABLE (
    response STRING, 
    sources ARRAY(STRING),
    chunks ARRAY(VARIANT),
    top_chunk VARIANT
)
LANGUAGE SQL
AS
$$
WITH embedded_query AS (
    SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m-v1.5', 
        query_text
    )::VECTOR(FLOAT, 768) AS query_vector
),
ranked_chunks AS (
    SELECT 
        FILE_NAME,
        COMBINED_CHUNK_TEXT,
        VECTOR_COSINE_SIMILARITY(COMBINED_CHUNK_VECTOR, query_vector) AS similarity
    FROM DOCUMENTATIONS_CHUNKED_VECTORS, embedded_query
    QUALIFY ROW_NUMBER() OVER (ORDER BY similarity DESC) <= 5
),
aggregated_data AS (
    SELECT 
        LISTAGG(COMBINED_CHUNK_TEXT, '\n\n') AS context,
        ARRAY_AGG(DISTINCT FILE_NAME)::ARRAY(STRING) AS sources,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'text', COMBINED_CHUNK_TEXT,
                'file', FILE_NAME,
                'similarity', similarity
            )::VARIANT
        )::ARRAY(VARIANT) AS chunks,
        ARRAY_AGG(
            OBJECT_CONSTRUCT(
                'text', COMBINED_CHUNK_TEXT,
                'file', FILE_NAME,
                'similarity', similarity
            )::VARIANT
        ) WITHIN GROUP (ORDER BY similarity DESC) AS ordered_chunks
    FROM ranked_chunks
)
SELECT
    SNOWFLAKE.CORTEX.COMPLETE(
        'mixtral-8x7b',
        'Context:\n' || context || '\nQuestion: ' || query_text
    ) AS response,
    sources,
    chunks,
    ordered_chunks[0]::VARIANT AS top_chunk
FROM aggregated_data
$$
;




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


-- Security setup (original grants preserved)
GRANT USAGE ON WAREHOUSE MEDIUM TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE LLM TO ROLE ACCOUNTADMIN;
