USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE Medium WAREHOUSE_SIZE='Medium' AUTO_SUSPEND = 300;
CREATE OR REPLACE DATABASE LLM;
CREATE OR REPLACE SCHEMA RAG;

USE LLM.RAG;



CREATE STAGE DOCUMENTATIONS;

LIST @LLM.RAG.DOCUMENTATIONS;

CREATE OR REPLACE FUNCTION py_read_markdown(file string)
    returns string 
    language python
    runtime_version = '3.8'
    packages = ('snowflake-snowpark-python', 'markdown', 'mistune')
    handler = 'read_file'
as 
$$
import mistune
from snowflake.snowpark.files import SnowflakeFile
from html.parser import HTMLParser

def read_file(file_path):
    with SnowflakeFile.open(file_path, 'r') as file:
        markdown_content = file.read()
        
        # Use mistune without explicit renderer
        html_content = mistune.html(markdown_content)  # Changed this line
        
        # Strip HTML tags
        class MLStripper(HTMLParser):
            def __init__(self):  # Fixed the method name formatting
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
SHOW USER FUNCTIONS;




CREATE OR REPLACE TABLE documentations AS
    WITH filenames AS (SELECT DISTINCT METADATA$FILENAME AS file_name FROM @documentations)
    SELECT 
        file_name, 
        py_read_markdown(build_scoped_file_url(@documentations, file_name)) AS contents
    FROM filenames;

--Validate
SELECT * FROM documentations;


---------Chinking------------
SET chunk_size = 3000;
SET overlap = 1000;
CREATE OR REPLACE TABLE documentations_chunked AS 
WITH RECURSIVE split_contents AS (
    SELECT 
        file_name,
        SUBSTRING(contents, 1, $chunk_size) AS chunk_text,
        SUBSTRING(contents, $chunk_size-$overlap) AS remaining_contents,
        1 AS chunk_number
    FROM 
        documentations

    UNION ALL

    SELECT 
        file_name,
        SUBSTRING(remaining_contents, 1, $chunk_size),
        SUBSTRING(remaining_contents, $chunk_size+1),
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

--Validate
SELECT * FROM documentations_chunked;

SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', 'hello world');
-------------------Vectorize-------------

CREATE OR REPLACE TABLE documentations_chunked_vectors AS 
SELECT 
    file_name, 
    chunk_number, 
    chunk_text, 
    combined_chunk_text,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', combined_chunk_text) as combined_chunk_vector
FROM 
    documentations_chunked;

--Validate
SELECT * FROM documentations_chunked_vectors;


------------------LLM Prompting----------------

set prompt = 'Give me code on how to use snowflake and propel?';

CREATE OR REPLACE FUNCTION DOCUMENTATIONS_LLM(prompt string)
RETURNS TABLE (response string, file_name string, chunk_text string, chunk_number int, score float)
AS
    $$
    WITH best_match_chunk AS (
        SELECT
            v.file_name,
            v.chunk_number,
            v.chunk_text,
            VECTOR_COSINE_SIMILARITY(v.combined_chunk_vector, snowflake.cortex.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', prompt)) AS score
        FROM 
            documentations_chunked_vectors v
        ORDER BY 
            score DESC
        LIMIT 10
    )
    SELECT 
        SNOWFLAKE.cortex.COMPLETE('mixtral-8x7b', 
            CONCAT('Answer this question: ', prompt, '\n\nUsing this documentations: ', chunk_text)
        ) AS response,
        file_name,
        chunk_text,
        chunk_number,
        score
    FROM
        best_match_chunk
    $$;

  -- Test the LLM:
SELECT * FROM TABLE(DOCUMENTATIONS_LLM($prompt));


