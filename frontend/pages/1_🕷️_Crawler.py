import streamlit as st
from services.api_client import APIClient
from components.url_input import render_url_input
from components.url_selector import render_url_selector
from components.results import render_results
from snowflake.snowpark.context import get_active_session
import urllib.parse
from snowflake.snowpark import Session
conn = st.connection("snowflake")
def initialize_session_state():
    """Initialize all session state variables with default values"""
    if "discovered_urls" not in st.session_state:
        st.session_state.discovered_urls = None
        
    if "domain" not in st.session_state:
        st.session_state.domain = None
        
    if "crawl_results" not in st.session_state:
        st.session_state.crawl_results = None
        
    if "crawled_domains" not in st.session_state:
        st.session_state.crawled_domains = set()
# Initialize API client
@st.cache_resource
def get_api_client():
    return APIClient()




@st.cache_resource
def init_snowflake():
    try:
        # Create session parameters from secrets
        session_parameters = {
            "account": st.secrets["snowflake"]["account"],
            "user": st.secrets["snowflake"]["user"],
            "warehouse": st.secrets["snowflake"]["warehouse"],
            "database": st.secrets["snowflake"]["database"],
            "schema": st.secrets["snowflake"]["schema"]
        }
        
        # Add authentication method based on what's in secrets
        if "password" in st.secrets["snowflake"]:
            session_parameters["password"] = st.secrets["snowflake"]["password"]
        elif "private_key" in st.secrets["snowflake"]:
            session_parameters["private_key"] = st.secrets["snowflake"]["private_key"]
        elif "authenticator" in st.secrets["snowflake"]:
            session_parameters["authenticator"] = st.secrets["snowflake"]["authenticator"]
            
        # Create and return the session
        session = Session.builder.configs(session_parameters).create()
        return session
    
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None


def process_crawl_results(session, results):
    """Process crawled results into Snowflake RAG tables"""
    try:
        for result in results:
            if not result.get('success'):
                continue
                
            # Extract domain from URL
            domain = urllib.parse.urlparse(result['url']).netloc
            
            # Add to crawled domains
            st.session_state.crawled_domains.add(domain)
            
            # Insert into RAG tables
            cmd = """
            INSERT INTO LLM.RAG.documentations 
            SELECT 
                %(url)s as url,
                %(content)s as contents
            """
            session.sql(cmd).bind({
                'url': result['url'],
                'content': result.get('markdown', '')
            }).collect()
            
            # Create chunks and vectors
            cmd = """
            INSERT INTO LLM.RAG.documentations_chunked
            WITH RECURSIVE split_contents AS (
                SELECT 
                    url,
                    SUBSTRING(contents, 1, 3000) AS chunk_text,
                    SUBSTRING(contents, 2000) AS remaining_contents,
                    1 AS chunk_number
                FROM 
                    LLM.RAG.documentations
                WHERE 
                    url = %(url)s
                    
                UNION ALL
                
                SELECT 
                    url,
                    SUBSTRING(remaining_contents, 1, 3000),
                    SUBSTRING(remaining_contents, 2000),
                    chunk_number + 1
                FROM 
                    split_contents
                WHERE 
                    LENGTH(remaining_contents) > 0
            )
            SELECT 
                url,
                chunk_number,
                chunk_text,
                CONCAT(
                    'Content from page [', 
                    url,
                    ']: ', 
                    chunk_text
                ) AS combined_chunk_text
            FROM 
                split_contents
            """
            session.sql(cmd).bind({'url': result['url']}).collect()
            
            # Create vectors
            cmd = """
            INSERT INTO LLM.RAG.documentations_chunked_vectors
            SELECT 
                url, 
                chunk_number, 
                chunk_text, 
                combined_chunk_text,
                SNOWFLAKE.CORTEX.EMBED_TEXT_768(
                    'snowflake-arctic-embed-m-v1.5', 
                    combined_chunk_text
                ) as combined_chunk_vector
            FROM 
                LLM.RAG.documentations_chunked
            WHERE 
                url = %(url)s
            """
            session.sql(cmd).bind({'url': result['url']}).collect()
            
        st.success("Successfully processed content for RAG")
        
    except Exception as e:
        st.error(f"Error processing results for RAG: {str(e)}")

def main():
    st.title("🕷️ Web Crawler")
    
    initialize_session_state()
    # Initialize API client and Snowflake
    api_client = get_api_client()
    session = init_snowflake()
    if not session:
        st.stop()
    
    # URL Input Phase
    url, mode, discover_clicked = render_url_input()
    
    if discover_clicked:
        with st.spinner("Discovering URLs..."):
            try:
                response = api_client.discover_urls(url, mode)
                st.session_state.discovered_urls = response['urls']
                st.session_state.domain = response['domain']
                st.success(f"Found {len(response['urls'])} URLs")
            except Exception:
                st.session_state.discovered_urls = None
                st.session_state.domain = None
    
    # URL Selection Phase
    if st.session_state.discovered_urls:
        selected_urls, exclude_patterns = render_url_selector(
            st.session_state.discovered_urls,
            st.session_state.domain
        )
        
        # Crawling Phase
        if selected_urls:
            if st.button("Start Crawling", type="primary"):
                with st.spinner("Crawling selected URLs..."):
                    try:
                        response = api_client.crawl_urls(selected_urls, exclude_patterns)
                        st.session_state.crawl_results = response['results']
                        
                        # Process results for RAG
                        with st.spinner("Processing content for RAG..."):
                            process_crawl_results(session, st.session_state.crawl_results)
                            
                    except Exception:
                        st.session_state.crawl_results = None
        
        # Results Phase
        if st.session_state.crawl_results:
            render_results(st.session_state.crawl_results)
            
            if st.button("Start Over"):
                st.session_state.discovered_urls = None
                st.session_state.domain = None
                st.session_state.crawl_results = None
                st.rerun()

if __name__ == "__main__":
    main()