import streamlit as st
import snowflake.connector

# Function to establish a connection to Snowflake and set the correct database and schema.
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user="TAZ16",
        password="Srinijani@95",
        account="blcquff-dcb49840",
        warehouse="MEDIUM",
        database="LLM",
        schema="LLM.RAG",
        role="ACCOUNTADMIN"
    )
    cur = conn.cursor()
    # Explicitly set the database and schema to avoid ambiguity.
    cur.execute("USE DATABASE LLM")
    cur.execute("USE SCHEMA RAG")
    cur.close()
    return conn

# Retrieve the list of URLs from CRAWL_METADATA.
def get_urls():
    conn = get_snowflake_connection()
    cur = conn.cursor()
    # Fully qualify the table name.
    cur.execute("SELECT URL FROM LLM.RAG.CRAWL_METADATA")
    results = cur.fetchall()
    urls = [row[0] for row in results]
    cur.close()
    conn.close()
    return urls

# Check if the selected URL is already chunked by counting chunks in DOCUMENTATIONS_CHUNKED_VECTORS.
def get_chunk_count(selected_url):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    # Fully qualify the table name here as well.
    cur.execute("SELECT COUNT(*) FROM LLM.RAG.DOCUMENTATIONS_CHUNKED_VECTORS WHERE URL = %s", (selected_url,))
    count = cur.fetchone()[0]
    cur.close()
    conn.close()
    return count

# Retrieve the markdown content for the selected URL.
def get_markdown(selected_url):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    cur.execute("SELECT MARKDOWN FROM LLM.RAG.CRAWL_METADATA WHERE URL = %s", (selected_url,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result and result[0]:
        return result[0]
    return "No markdown content available."

# Call the Snowflake procedures to chunk the URL and update embeddings.
def chunk_and_index(selected_url):
    conn = get_snowflake_connection()
    cur = conn.cursor()
    # Fully qualify the procedure calls if necessary.
    cur.execute(f"CALL LLM.RAG.CREATE_DOCUMENT_CHUNKS(ARRAY_CONSTRUCT('{selected_url}'))")
    cur.execute(f"CALL LLM.RAG.UPDATE_EMBEDDINGS(ARRAY_CONSTRUCT('{selected_url}'))")
    conn.commit()
    cur.close()
    conn.close()

# Main Streamlit app layout.
st.title("Snowflake URL Chunking and Indexing")

# Sidebar: Display list of URLs from the CRAWL_METADATA table.
urls = get_urls()
selected_url = st.sidebar.selectbox("Select a URL", urls)

if selected_url:
    st.header("URL Information")

    # Check the number of chunks for the selected URL.
    chunk_count = get_chunk_count(selected_url)
    if chunk_count > 0:
        st.write(f"The URL is already chunked and indexed. Number of chunks: {chunk_count}")
    else:
        st.write("This URL has not been chunked and indexed yet.")
        # When the button is clicked, call the procedures.
        if st.button("Chunk and Index This URL"):
            with st.spinner("Processing the URL..."):
                chunk_and_index(selected_url)
            st.success("Chunking and indexing complete!")
            # After processing, update and display the new chunk count.
            chunk_count = get_chunk_count(selected_url)
            st.write(f"Number of chunks after indexing: {chunk_count}")
    
    # Divider between metadata information and markdown.
    st.markdown("---")
    st.subheader("Markdown Content")
    markdown_content = get_markdown(selected_url)
    st.markdown(markdown_content)
