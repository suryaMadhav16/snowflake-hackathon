USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE Medium WAREHOUSE_SIZE='Medium' AUTO_SUSPEND = 300;
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA RAG;

USE LLM.RAG;

-- Create stage for documentations
CREATE OR REPLACE STAGE DOCUMENTATIONS;

-- Create main documentations table
CREATE OR REPLACE TABLE DOCUMENTATIONS (
    FILE_NAME TEXT NOT NULL PRIMARY KEY,
    CONTENTS TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Create chunked table
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED (
    FILE_NAME TEXT NOT NULL,
    CHUNK_NUMBER NUMBER NOT NULL,
    CHUNK_TEXT TEXT,
    COMBINED_CHUNK_TEXT TEXT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Create vectors table
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
    FILE_NAME TEXT NOT NULL,
    CHUNK_NUMBER NUMBER NOT NULL,
    CHUNK_TEXT TEXT,
    COMBINED_CHUNK_TEXT TEXT,
    COMBINED_CHUNK_VECTOR ARRAY,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (FILE_NAME, CHUNK_NUMBER)
);

-- Create CRAWL_METADATA table
CREATE OR REPLACE TABLE CRAWL_METADATA (
    URL TEXT NOT NULL PRIMARY KEY,
    SUCCESS BOOLEAN,
    ERROR_MESSAGE TEXT,
    METADATA OBJECT,
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    FILE_NAME TEXT,
    FILE_TYPE TEXT,
    CONTENT_TYPE TEXT,
    SIZE NUMBER
);

-- Create markdown processing function
CREATE OR REPLACE FUNCTION PY_READ_MARKDOWN(file string)
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

-- Create sync procedure with improved error handling
CREATE OR REPLACE PROCEDURE SYNC_CRAWL_CONTENT()
    RETURNS VARIANT
    LANGUAGE SQL
AS
$
DECLARE
    result VARIANT;
    rows_inserted INTEGER DEFAULT 0;
BEGIN
    -- Merge markdown content to DOCUMENTATIONS table
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

    -- Create chunks for new documents
    INSERT INTO DOCUMENTATIONS_CHUNKED (
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        COMBINED_CHUNK_TEXT
    )
    SELECT 
        d.FILE_NAME,
        ROW_NUMBER() OVER (PARTITION BY d.FILE_NAME ORDER BY pos) as CHUNK_NUMBER,
        SUBSTR(d.CONTENTS, pos, 3000) as CHUNK_TEXT,
        CONCAT('Content from document [', d.FILE_NAME, ']: ', SUBSTR(d.CONTENTS, pos, 3000)) as COMBINED_CHUNK_TEXT
    FROM DOCUMENTATIONS d
    CROSS JOIN TABLE(GENERATOR(ROWCOUNT => (LENGTH(d.CONTENTS) + 2999) / 3000)) g
    LEFT JOIN DOCUMENTATIONS_CHUNKED dc
        ON d.FILE_NAME = dc.FILE_NAME
    WHERE dc.FILE_NAME IS NULL
    AND pos <= LENGTH(d.CONTENTS);

    -- Generate embeddings for new chunks
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
    SELECT
        OBJECT_CONSTRUCT(
            'status', 'success',
            'message', 'Content sync completed successfully',
            'rows_processed', rows_inserted
        ) INTO :result;

    RETURN result;

EXCEPTION
    WHEN OTHER THEN
        SELECT
            OBJECT_CONSTRUCT(
                'status', 'error',
                'message', 'Content sync failed: ' || SQLERRM,
                'error_code', SQLCODE,
                'error_state', SQLSTATE
            ) INTO :result;
        RETURN result;
END;
$;


-- Create chunking procedure
CREATE OR REPLACE PROCEDURE CHUNK_DOCUMENTS(
    chunk_size NUMBER DEFAULT 3000,
    overlap_size NUMBER DEFAULT 1000
)
RETURNS OBJECT
LANGUAGE SQL
EXECUTE AS CALLER
AS
DECLARE
    result OBJECT;
BEGIN
    -- Initialize result
    LET result := OBJECT_CONSTRUCT(
        'status', 'pending',
        'chunks_created', 0,
        'error', NULL
    );
    
    -- Start transaction
    BEGIN TRANSACTION;
    
    -- Clear existing chunks
    TRUNCATE TABLE DOCUMENTATIONS_CHUNKED;
    
    -- Create new chunks
    INSERT INTO DOCUMENTATIONS_CHUNKED (FILE_NAME, CHUNK_NUMBER, CHUNK_TEXT, COMBINED_CHUNK_TEXT)
    WITH RECURSIVE chunks AS (
        SELECT 
            FILE_NAME,
            CONTENTS,
            1 as CHUNK_NUMBER,
            SUBSTRING(CONTENTS, 1, chunk_size) as CHUNK_TEXT
        FROM DOCUMENTATIONS
        
        UNION ALL
        
        SELECT 
            FILE_NAME,
            CONTENTS,
            CHUNK_NUMBER + 1,
            SUBSTRING(
                CONTENTS, 
                (CHUNK_NUMBER * chunk_size) - overlap_size, 
                chunk_size
            )
        FROM chunks
        WHERE LENGTH(SUBSTRING(
            CONTENTS, 
            (CHUNK_NUMBER * chunk_size) - overlap_size, 
            chunk_size
        )) > 0
    )
    SELECT 
        FILE_NAME,
        CHUNK_NUMBER,
        CHUNK_TEXT,
        CONCAT('Content from document [', FILE_NAME, ']: ', CHUNK_TEXT) as COMBINED_CHUNK_TEXT
    FROM chunks
    ORDER BY FILE_NAME, CHUNK_NUMBER;
    
    -- Get number of chunks created
    LET chunks_created := SQLROWCOUNT;
    
    -- Commit transaction
    COMMIT;
    
    -- Update result
    LET result := OBJECT_CONSTRUCT(
        'status', 'success',
        'chunks_created', chunks_created,
        'error', NULL
    );
    
    RETURN result;

EXCEPTION
    WHEN OTHER THEN
        -- Roll back on error
        ROLLBACK;
        
        -- Update result with error info
        LET result := OBJECT_CONSTRUCT(
            'status', 'error',
            'chunks_created', 0,
            'error', OBJECT_CONSTRUCT(
                'code', SQLCODE,
                'state', SQLSTATE,
                'message', SQLERRM
            )
        );
        
        RETURN result;
END;
