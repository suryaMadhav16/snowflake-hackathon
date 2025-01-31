import streamlit as st
from services.api_client import APIClient
from components.url_input import render_url_input
from components.url_selector import render_url_selector
from components.results import render_results
from snowflake.snowpark import Session
import urllib.parse
import logging
import traceback
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler_frontend.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

conn = st.connection("snowflake")

def initialize_session_state():
    if "discovered_urls" not in st.session_state:
        st.session_state.discovered_urls = None
    if "domain" not in st.session_state:
        st.session_state.domain = None
    if "crawl_results" not in st.session_state:
        st.session_state.crawl_results = None
    if "crawled_domains" not in st.session_state:
        st.session_state.crawled_domains = set()

@st.cache_resource
def get_api_client():
    return APIClient()

@st.cache_resource
def init_snowflake():
    """Initialize Snowflake session with error handling and logging"""
    logger.info("Initializing Snowflake session...")
    try:
        session_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        if "password" in st.secrets["snowflake"]:
            session_parameters["password"] = st.secrets["snowflake"]["password"]
        elif "private_key" in st.secrets["snowflake"]:
            session_parameters["private_key"] = st.secrets["snowflake"]["private_key"]
        elif "authenticator" in st.secrets["snowflake"]:
            session_parameters["authenticator"] = st.secrets["snowflake"]["authenticator"]
            
        session = Session.builder.configs(session_parameters).create()
        logger.info("Successfully connected to Snowflake")
        
        # Ensure the newly-added DOCUMENTATIONS_CHUNKED table exists:
        session.sql("""
            CREATE TABLE IF NOT EXISTS LLM.RAG.DOCUMENTATIONS_CHUNKED (
                url TEXT,
                chunk_number NUMBER,
                chunk_text TEXT,
                combined_chunk_text TEXT,
                CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """).collect()
        
        return session
    
    except Exception as e:
        error_details = traceback.format_exc()
        logger.error(f"Snowflake connection failed: {str(e)}\n{error_details}")
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

def process_crawl_results(session, results):
    """Insert raw content into DOCUMENTATIONS, split into chunks, and create vectors."""
    logger.info(f"Starting to process {len(results)} crawl results")
    try:
        for index, result in enumerate(results):
            logger.debug(f"Processing result {index + 1}/{len(results)}")
            if not result.get("success"):
                continue
            
            domain = urllib.parse.urlparse(result["url"]).netloc
            st.session_state.crawled_domains.add(domain)
            
            # 1) Insert raw markdown into DOCUMENTATIONS
            insert_docs_sql = """
                INSERT INTO LLM.RAG.DOCUMENTATIONS (FILE_NAME, CONTENTS)
                VALUES (?, ?)
            """
            try:
                session.sql(insert_docs_sql, params=[
                    result["url"],
                    result.get("markdown", "")
                ]).collect()
            except Exception as e:
                logger.error(f"Failed to insert data for URL {result['url']}: {str(e)}\n{traceback.format_exc()}")
                continue
            
            # 2) Chunk the newly-inserted content and store in DOCUMENTATIONS_CHUNKED
            #    using SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER
            chunk_sql = """
                INSERT INTO LLM.RAG.DOCUMENTATIONS_CHUNKED (url, chunk_number, chunk_text, combined_chunk_text)
                SELECT 
                    d.FILE_NAME AS url,
                    ROW_NUMBER() OVER (PARTITION BY d.FILE_NAME ORDER BY chunk.seq) AS chunk_number,
                    chunk.value::TEXT AS chunk_text,
                    CONCAT('Content from page [', d.FILE_NAME, ']: ', chunk.value::TEXT) AS combined_chunk_text
                FROM LLM.RAG.DOCUMENTATIONS d,
                     LATERAL FLATTEN(input => SNOWFLAKE.CORTEX.SPLIT_TEXT_RECURSIVE_CHARACTER(
                         d.CONTENTS,
                         'markdown',
                         512,
                         50
                     )) chunk
                WHERE d.FILE_NAME = ?
            """
            try:
                session.sql(chunk_sql, params=[result["url"]]).collect()
            except Exception as e:
                logger.error(f"Failed to chunk content for {result['url']}: {str(e)}\n{traceback.format_exc()}")
                continue
            
            # 3) Generate embeddings from the chunked data
            vector_sql = """
                INSERT INTO LLM.RAG.DOCUMENTATIONS_CHUNKED_VECTORS (
                    FILE_NAME,
                    CHUNK_NUMBER,
                    COMBINED_CHUNK_TEXT,
                    COMBINED_CHUNK_VECTOR
                )
                SELECT 
                    url AS FILE_NAME,
                    chunk_number,
                    combined_chunk_text,
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                        'snowflake-arctic-embed-m-v1.5',
                        combined_chunk_text
                    )::VECTOR(FLOAT, 768)
                FROM LLM.RAG.DOCUMENTATIONS_CHUNKED
                WHERE url = ?
            """
            try:
                session.sql(vector_sql, params=[result["url"]]).collect()
            except Exception as e:
                logger.error(f"Failed to generate vectors for URL {result['url']}: {str(e)}\n{traceback.format_exc()}")
                raise
            
        logger.info("Successfully completed processing all results")
        st.success("Successfully processed content for RAG")
        
    except Exception as e:
        error_msg = f"Error processing results for RAG: {str(e)}"
        logger.error(f"{error_msg}\n{traceback.format_exc()}")
        st.error(error_msg)

def main():
    logger.info("Starting crawler application")
    st.title("🕷️ Web Crawler")
    
    initialize_session_state()
    api_client = get_api_client()
    session = init_snowflake()
    if not session:
        st.stop()
    
    url, mode, discover_clicked = render_url_input()
    
    if discover_clicked:
        with st.spinner("Discovering URLs..."):
            try:
                response = api_client.discover_urls(url, mode)
                st.session_state.discovered_urls = response["urls"]
                st.session_state.domain = response["domain"]
                st.success(f"Found {len(response['urls'])} URLs")
            except Exception as e:
                error_msg = f"Failed to discover URLs: {str(e)}"
                logger.error(f"{error_msg}\n{traceback.format_exc()}")
                st.error(error_msg)
                st.session_state.discovered_urls = None
                st.session_state.domain = None
    
    if st.session_state.discovered_urls:
        selected_urls, exclude_patterns = render_url_selector(
            st.session_state.discovered_urls,
            st.session_state.domain
        )
        if selected_urls:
            if st.button("Start Crawling", type="primary"):
                with st.spinner("Crawling selected URLs..."):
                    try:
                        response = api_client.crawl_urls(selected_urls, exclude_patterns)
                        st.session_state.crawl_results = response["results"]
                        
                        with st.spinner("Processing content for RAG..."):
                            process_crawl_results(session, st.session_state.crawl_results)
                    except Exception as e:
                        error_msg = f"Crawling failed: {str(e)}"
                        logger.error(f"{error_msg}\n{traceback.format_exc()}")
                        st.error(error_msg)
                        st.session_state.crawl_results = None
        
        if st.session_state.crawl_results:
            render_results(st.session_state.crawl_results)
            
            if st.button("Start Over"):
                st.session_state.discovered_urls = None
                st.session_state.domain = None
                st.session_state.crawl_results = None
                st.rerun()

if __name__ == "__main__":
    main()
