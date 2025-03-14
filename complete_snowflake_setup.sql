-- Snowflake Environment Setup Script
-- This script sets up a data processing environment for LLM-based RAG system

-- Warehouse Configuration
-- Setup warehouse optimized for vector processing operations
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE MEDIUM 
    WAREHOUSE_SIZE = 'LARGE'  -- For better vector processing
    AUTO_SUSPEND = 600;       -- Extended timeout (10 minutes)

-- Database and Schema Initialization
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA LLM.RAG;
USE SCHEMA LLM.RAG;

-- Metadata Table for Web Crawling Results
-- Stores information about crawled documents and their properties
CREATE OR REPLACE TABLE CRAWL_METADATA (
    URL TEXT NOT NULL PRIMARY KEY,
    SUCCESS BOOLEAN,          -- Indicates if crawl was successful
    ERROR_MESSAGE TEXT,       -- Stores any error messages during crawling
    METADATA VARIANT,         -- Flexible JSON metadata storage
    TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    MARKDOWN VARCHAR(2000000),  -- Stores raw markdown content
    FILE_NAME TEXT,
    FILE_TYPE TEXT,
    CONTENT_TYPE TEXT,
    SIZE NUMBER    
);

-- Vector Storage Table
-- Stores text chunks and their vector embeddings for semantic search
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
  CHUNK_ID INT AUTOINCREMENT,          -- Unique identifier for each chunk
  URL TEXT NOT NULL,                   -- Reference to source document
  CHUNK_NUMBER NUMBER NOT NULL,        -- Position in the original document
  COMBINED_CHUNK_TEXT TEXT,            -- The actual text chunk
  COMBINED_CHUNK_VECTOR VECTOR(FLOAT, 768),  -- Vector embedding
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (CHUNK_ID),
  FOREIGN KEY (URL) REFERENCES CRAWL_METADATA (URL)
);

-- Document Processing Procedures and Functions

-- Chunks documents into smaller segments for processing
CREATE OR REPLACE PROCEDURE CREATE_DOCUMENT_CHUNKS(user_urls ARRAY)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
  INSERT INTO DOCUMENTATIONS_CHUNKED_VECTORS (URL, CHUNK_NUMBER, COMBINED_CHUNK_TEXT)
  SELECT 
      cm.URL,
      chunk.INDEX AS CHUNK_NUMBER,
      chunk.VALUE::TEXT AS COMBINED_CHUNK_TEXT
  FROM CRAWL_METADATA cm,
       LATERAL FLATTEN(
         INPUT => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                     cm.MARKDOWN,     -- Source markdown text
                     'markdown',      -- Input format
                     5000,           -- Target chunk size
                     500             -- Overlap between chunks
                 )
       ) AS chunk
  WHERE cm.URL IN (
    SELECT VALUE::TEXT FROM TABLE(FLATTEN(INPUT => :user_urls))
  );
  
  RETURN 'Chunks inserted successfully.';
END;
$$
;

-- Generates vector embeddings for input text
CREATE OR REPLACE FUNCTION GENERATE_EMBEDDING(text_input STRING)
  RETURNS VECTOR(FLOAT, 768)
  LANGUAGE SQL
AS
$$
  SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', text_input)
$$
;

-- Updates vector embeddings for specified document chunks
CREATE OR REPLACE PROCEDURE UPDATE_EMBEDDINGS(user_urls ARRAY)
  RETURNS STRING
  LANGUAGE SQL
AS
$$
BEGIN
  UPDATE DOCUMENTATIONS_CHUNKED_VECTORS
  SET COMBINED_CHUNK_VECTOR = GENERATE_EMBEDDING(COMBINED_CHUNK_TEXT)
  WHERE URL IN (
    SELECT VALUE::TEXT FROM TABLE(FLATTEN(INPUT => :user_urls))
  );
  
  RETURN 'Embedding vectors updated successfully.';
END;
$$
;

-- Query Response Generation Procedures

-- Generates context-aware responses using semantic search and LLM
CREATE OR REPLACE PROCEDURE GENERATE_CONTEXT_RESPONSE(user_query STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'generate_context_response'
AS
$$
import json

def generate_context_response(session, user_query: str) -> str:
    # 1. Generate query embedding
    result = session.sql(
        "SELECT GENERATE_EMBEDDING(?) AS QUERY_EMBEDDING",
        (user_query,)
    ).collect()
    if not result:
        return json.dumps({
            "generated_response": "No context available", 
            "chunks": [], 
            "user_query": user_query
        })
    query_embedding = result[0]["QUERY_EMBEDDING"]
    if not query_embedding:
        return json.dumps({
            "generated_response": "No context available", 
            "chunks": [], 
            "user_query": user_query
        })
    
    # 2. Prepare embedding for vector similarity search
    embedding_values = ", ".join([str(x) for x in query_embedding])
    embedding_literal = f"ARRAY_CONSTRUCT({embedding_values})::vector(FLOAT,768)"
    
    # 3. Retrieve relevant context chunks
    sql_query = f"""
        SELECT COMBINED_CHUNK_TEXT
        FROM DOCUMENTATIONS_CHUNKED_VECTORS
        ORDER BY VECTOR_COSINE_SIMILARITY(COMBINED_CHUNK_VECTOR, {embedding_literal}) DESC
        LIMIT 3
    """
    context_chunks = session.sql(sql_query).collect()
    if not context_chunks:
        return json.dumps({
            "generated_response": "No context available", 
            "chunks": [], 
            "user_query": user_query
        })
    
    # 4. Process and combine context chunks
    chunk_texts = [row["COMBINED_CHUNK_TEXT"] for row in context_chunks]
    context_text = "\n\n".join(chunk_texts)
    prompt = f"Context:\n{context_text}\n\nQuery: {user_query}"
    
    # 5. Generate LLM response
    complete_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', ?) AS RESPONSE"
    complete_result = session.sql(complete_sql, (prompt,)).collect()
    if not complete_result:
        return json.dumps({
            "generated_response": "No response generated", 
            "chunks": chunk_texts, 
            "user_query": user_query
        })
    response = complete_result[0]["RESPONSE"]
    
    return json.dumps({
        "generated_response": response,
        "chunks": chunk_texts,
        "user_query": user_query
    })
$$
;

-- Retrieves semantically relevant document chunks for a query
CREATE OR REPLACE PROCEDURE GET_RELEVANT_CHUNKS(user_query STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'get_relevant_chunks_proc'
AS
$$
import json

def get_relevant_chunks_proc(session, user_query: str) -> str:
    # Generate query embedding
    embedding_result = session.sql(
        "SELECT GENERATE_EMBEDDING(?) AS QUERY_EMBEDDING",
        (user_query,)
    ).collect()
    if not embedding_result or embedding_result[0]["QUERY_EMBEDDING"] is None:
        return json.dumps([])
    
    query_embedding = embedding_result[0]["QUERY_EMBEDDING"]
    
    # Prepare embedding for similarity search
    embedding_values = ", ".join([str(x) for x in query_embedding])
    embedding_literal = f"ARRAY_CONSTRUCT({embedding_values})::vector(FLOAT,768)"
    
    # Retrieve top 5 similar chunks with scores
    sql_query = f"""
        SELECT COMBINED_CHUNK_TEXT,
               VECTOR_COSINE_SIMILARITY(COMBINED_CHUNK_VECTOR, {embedding_literal}) AS SCORE
        FROM DOCUMENTATIONS_CHUNKED_VECTORS
        ORDER BY SCORE DESC
        LIMIT 5
    """
    chunk_result = session.sql(sql_query).collect()
    if not chunk_result:
        return json.dumps([])
    
    # Format results
    output = []
    for row in chunk_result:
        output.append({
            "chunk": row["COMBINED_CHUNK_TEXT"],
            "score": row["SCORE"]
        })
    return json.dumps(output)
$$
;

-- Direct query answering without context
CREATE OR REPLACE PROCEDURE ANSWER_QUERY(user_query STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'answer_query_proc'
AS
$$
def answer_query_proc(session, user_query: str) -> str:
    # Direct LLM query without context retrieval
    complete_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', ?) AS RESPONSE"
    complete_result = session.sql(complete_sql, (user_query,)).collect()
    if not complete_result:
        return "No response generated"
    return complete_result[0]["RESPONSE"]
$$
;

-- Security Configuration
GRANT USAGE ON WAREHOUSE MEDIUM TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE LLM TO ROLE ACCOUNTADMIN;
