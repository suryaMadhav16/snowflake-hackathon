-- Create sync procedure with improved error handling
CREATE OR REPLACE PROCEDURE SYNC_CRAWL_CONTENT()
    RETURNS VARIANT
    LANGUAGE SQL
AS
$$
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

    -- Create chunks for new documents using SPLIT function
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
            CHUNK_NUMBER,
            1 + ((CHUNK_NUMBER - 1) * 3000) as chunk_start,
            LEAST(LENGTH(CONTENTS), CHUNK_NUMBER * 3000) as chunk_end
        FROM document_lengths,
        TABLE(GENERATOR(rowcount => 100)) -- Adjust based on max expected chunks
        WHERE CHUNK_NUMBER <= num_chunks
    )
    SELECT 
        FILE_NAME,
        CHUNK_NUMBER,
        SUBSTRING(CONTENTS, chunk_start, chunk_end - chunk_start + 1) as CHUNK_TEXT,
        CONCAT('Content from document [', FILE_NAME, ']: ', 
               SUBSTRING(CONTENTS, chunk_start, chunk_end - chunk_start + 1)) as COMBINED_CHUNK_TEXT
    FROM chunk_ranges;

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
