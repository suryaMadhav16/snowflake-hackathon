-- Warehouse setup with vector processing optimizations
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE WAREHOUSE MEDIUM 
    WAREHOUSE_SIZE = 'LARGE'  -- For better vector processing
    AUTO_SUSPEND = 600;       -- Extended timeout

-- Database and schema setup (original names preserved)
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA LLM.RAG;
USE SCHEMA LLM.RAG;

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
ALTER TABLE CRAWL_METADATA 
ADD COLUMN MARKDOWN VARCHAR(2000000);


-- New
CREATE OR REPLACE TABLE DOCUMENTATIONS_CHUNKED_VECTORS (
  CHUNK_ID INT AUTOINCREMENT,           -- Auto-incrementing primary key
  URL TEXT NOT NULL,                     -- Reference to the source URL in CRAWL_METADATA
  CHUNK_NUMBER NUMBER NOT NULL,
  COMBINED_CHUNK_TEXT TEXT,
  COMBINED_CHUNK_VECTOR VECTOR(FLOAT, 768),
  CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  PRIMARY KEY (CHUNK_ID),
  FOREIGN KEY (URL) REFERENCES CRAWL_METADATA (URL)
);


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
                     cm.MARKDOWN,     -- Source text to be chunked (e.g., markdown content)
                     'markdown',      -- Format specifier for handling markdown features
                     5000,            -- Desired chunk size
                     500              -- Overlap size in characters
                 )
       ) AS chunk
  WHERE cm.URL IN (
    SELECT VALUE::TEXT FROM TABLE(FLATTEN(INPUT => :user_urls))
  );
  
  RETURN 'Chunks inserted successfully.';
END;
$$

;
CREATE OR REPLACE FUNCTION GENERATE_EMBEDDING(text_input STRING)
  RETURNS VECTOR(FLOAT, 768)
  LANGUAGE SQL
AS
$$
  SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m', text_input)
$$
;


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
    # 1. Compute the query embedding using the pre-defined UDF.
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
    
    # 2. Convert the query embedding (a list of floats) into a literal.
    # Bind variables are not supported for VECTOR types, so build a literal.
    embedding_values = ", ".join([str(x) for x in query_embedding])
    embedding_literal = f"ARRAY_CONSTRUCT({embedding_values})::vector(FLOAT,768)"
    
    # 3. Retrieve the top three text chunks ordered by cosine similarity (most relevant first).
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
    
    # 4. Extract the text chunks from the result.
    chunk_texts = [row["COMBINED_CHUNK_TEXT"] for row in context_chunks]
    
    # 5. Aggregate the text chunks and create the prompt.
    context_text = "\n\n".join(chunk_texts)
    prompt = f"Context:\n{context_text}\n\nQuery: {user_query}"
    
    # 6. Call Cortex COMPLETE with the constructed prompt.
    complete_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', ?) AS RESPONSE"
    complete_result = session.sql(complete_sql, (prompt,)).collect()
    if not complete_result:
        return json.dumps({
            "generated_response": "No response generated", 
            "chunks": chunk_texts, 
            "user_query": user_query
        })
    response = complete_result[0]["RESPONSE"]
    
    # 7. Return the response, list of chunks, and the original query as a JSON string.
    output = {
        "generated_response": response,
        "chunks": chunk_texts,
        "user_query": user_query
    }
    return json.dumps(output)
$$
;

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
    # Compute the query embedding using the pre-defined UDF GENERATE_EMBEDDING.
    embedding_result = session.sql(
        "SELECT GENERATE_EMBEDDING(?) AS QUERY_EMBEDDING",
        (user_query,)
    ).collect()
    if not embedding_result or embedding_result[0]["QUERY_EMBEDDING"] is None:
        return json.dumps([])
    
    query_embedding = embedding_result[0]["QUERY_EMBEDDING"]
    
    # Convert the embedding (a list of floats) into a SQL literal.
    # Bind parameters are not supported for VECTOR types, so we build a literal.
    embedding_values = ", ".join([str(x) for x in query_embedding])
    embedding_literal = f"ARRAY_CONSTRUCT({embedding_values})::vector(FLOAT,768)"
    
    # Retrieve the top 5 chunks ordered by cosine similarity (most relevant first).
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
    
    # Build the output as a list of dictionaries with chunk text and relevancy score.
    output = []
    for row in chunk_result:
        output.append({
            "chunk": row["COMBINED_CHUNK_TEXT"],
            "score": row["SCORE"]
        })
    return json.dumps(output)
$$
;

CREATE OR REPLACE PROCEDURE ANSWER_QUERY(user_query STRING)
  RETURNS STRING
  LANGUAGE PYTHON
  RUNTIME_VERSION = '3.8'
  PACKAGES = ('snowflake-snowpark-python')
  HANDLER = 'answer_query_proc'
AS
$$
def answer_query_proc(session, user_query: str) -> str:
    # Use the user query directly as the prompt to generate a response.
    complete_sql = "SELECT SNOWFLAKE.CORTEX.COMPLETE('llama2-70b-chat', ?) AS RESPONSE"
    complete_result = session.sql(complete_sql, (user_query,)).collect()
    if not complete_result:
        return "No response generated"
    return complete_result[0]["RESPONSE"]
$$
;



-- Security setup (original grants preserved)
GRANT USAGE ON WAREHOUSE MEDIUM TO ROLE ACCOUNTADMIN;
GRANT ALL ON DATABASE LLM TO ROLE ACCOUNTADMIN;
