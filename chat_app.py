"""Documentation Assistant Streamlit Application.

This module implements a chat-based documentation assistant using Streamlit and Snowflake.
It provides functionality for:
1. Uploading and processing documentation files
2. Question-answering based on documentation context
3. Semantic search for relevant content
4. Report generation from chat conversations

The application uses Snowflake's vector similarity search and LLM capabilities
for processing and retrieving documentation.

Example:
    To run the application:
        $ streamlit run chat_app.py
"""

import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

def init_snowflake():
    """Initialize connection to Snowflake.

    Attempts to get an active Snowflake session for database operations.

    Returns:
        Session: Active Snowflake session if successful, None otherwise.

    Example:
        >>> session = init_snowflake()
        >>> if session:
        ...     print("Connected to Snowflake")
    """
    try:
        return get_active_session()
    except Exception as e:
        st.error(f"Failed to connect to Snowflake: {str(e)}")
        return None

def process_uploaded_file(session, uploaded_file):
    """Process an uploaded documentation file.

    Handles the complete workflow for processing new documentation:
    1. Uploads file to Snowflake stage
    2. Processes markdown content
    3. Chunks the content with overlap
    4. Generates vector embeddings

    Args:
        session: Active Snowflake session
        uploaded_file: StreamlitUploadedFile object

    Returns:
        bool: True if processing was successful, False otherwise.

    Example:
        >>> if process_uploaded_file(session, uploaded_file):
        ...     print("File processed successfully")
    """
    try:
        # 1. Upload to stage
        file_content = uploaded_file.read()
        stage_name = "@LLM.RAG.DOCUMENTATIONS"
        
        cmd = f"""
        PUT file://{uploaded_file.name} {stage_name}
        OVERWRITE = TRUE
        AUTO_COMPRESS = FALSE
        """
        session.sql(cmd).collect()
        
        # 2. Process the new file through the markdown reader
        cmd = f"""
        INSERT INTO LLM.RAG.documentations 
        SELECT 
            '{uploaded_file.name}' as file_name,
            LLM.RAG.py_read_markdown(build_scoped_file_url(@LLM.RAG.DOCUMENTATIONS, '{uploaded_file.name}')) AS contents
        """
        session.sql(cmd).collect()
        
        # 3. Create chunks
        chunk_size = 3000
        overlap = 1000
        
        cmd = f"""
        INSERT INTO LLM.RAG.documentations_chunked
        WITH RECURSIVE split_contents AS (
            SELECT 
                file_name,
                SUBSTRING(contents, 1, {chunk_size}) AS chunk_text,
                SUBSTRING(contents, {chunk_size}-{overlap}) AS remaining_contents,
                1 AS chunk_number
            FROM 
                LLM.RAG.documentations
            WHERE 
                file_name = '{uploaded_file.name}'

            UNION ALL

            SELECT 
                file_name,
                SUBSTRING(remaining_contents, 1, {chunk_size}),
                SUBSTRING(remaining_contents, {chunk_size}+1),
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
        """
        session.sql(cmd).collect()
        
        # 4. Create vectors
        cmd = f"""
        INSERT INTO LLM.RAG.documentations_chunked_vectors
        SELECT 
            file_name, 
            chunk_number, 
            chunk_text, 
            combined_chunk_text,
            SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', combined_chunk_text) as combined_chunk_vector
        FROM 
            LLM.RAG.documentations_chunked
        WHERE 
            file_name = '{uploaded_file.name}'
        """
        session.sql(cmd).collect()
        
        return True
        
    except Exception as e:
        st.error(f"Error processing file: {str(e)}")
        return False

def get_similar_chunks(session, question, num_chunks=3, similarity_threshold=0.7):
    """Retrieve document chunks similar to the input question.

    Uses vector similarity search to find relevant documentation chunks.

    Args:
        session: Active Snowflake session
        question (str): User's input question
        num_chunks (int, optional): Number of chunks to retrieve. Defaults to 3.
        similarity_threshold (float, optional): Minimum similarity score. Defaults to 0.7.

    Returns:
        DataFrame: Retrieved chunks with similarity scores.

    Example:
        >>> chunks = get_similar_chunks(session, "How to configure logging?")
        >>> print(f"Found {len(chunks)} relevant chunks")
    """
    cmd = """
        WITH best_match_chunk AS (
            SELECT
                v.FILE_NAME,
                v.CHUNK_NUMBER,
                v.CHUNK_TEXT,
                VECTOR_COSINE_SIMILARITY(
                    v.COMBINED_CHUNK_VECTOR, 
                    SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', ?)
                ) AS similarity
            FROM 
                documentations_chunked_vectors v
            HAVING 
                similarity >= ?
            ORDER BY 
                similarity DESC
            LIMIT ?
        )
        SELECT * FROM best_match_chunk
    """
    
    try:
        df_chunks = session.sql(cmd, params=[question, similarity_threshold, num_chunks]).to_pandas()
        return df_chunks
    except Exception as e:
        st.error(f"Error retrieving similar chunks: {str(e)}")
        return pd.DataFrame()

def generate_response(session, question, context):
    """Generate an answer using Snowflake's LLM capabilities.

    Uses the provided context to generate a relevant response to the question.

    Args:
        session: Active Snowflake session
        question (str): User's question
        context (str): Retrieved documentation context

    Returns:
        str: Generated response

    Example:
        >>> response = generate_response(session, "How to log errors?", context)
        >>> print(response)
    """
    prompt = f"""You are an expert assistant helping users understand documentation.
    Answer the question based on the following context. Be concise and accurate.
    If the context doesn't contain relevant information, say so.
    
    Context:
    {context}
    
    Question: {question}
    
    Answer:"""
    
    cmd = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', ?) as response
    """
    
    try:
        result = session.sql(cmd, params=[prompt]).collect()
        return result[0]["RESPONSE"]
    except Exception as e:
        st.error(f"Error generating response: {str(e)}")
        return "I encountered an error while generating the response."

def generate_markdown_report(session, messages):
    """Generate a structured markdown report from chat messages.

    Converts the chat conversation into a well-formatted technical document.

    Args:
        session: Active Snowflake session
        messages (List[Dict]): List of chat messages with role and content

    Returns:
        str: Generated markdown report

    Example:
        >>> report = generate_markdown_report(session, messages)
        >>> with open('report.md', 'w') as f:
        ...     f.write(report)
    """
    # Convert messages to a structured format for the prompt
    conversation = []
    for msg in messages:
        prefix = "User" if msg["role"] == "user" else "Assistant"
        conversation.append(f"{prefix}: {msg['content']}")
    
    conversation_text = "\n\n".join(conversation)
    
    prompt = f"""Based on the following conversation, create a well-structured technical report or how-to guide in Markdown format.
    If the conversation appears to be about instructions or procedures, structure it as a step-by-step guide.
    Include appropriate headers, code blocks, and formatting.
    Focus on the key technical insights and actionable information.

    Conversation:
    {conversation_text}

    Create a clear, professional markdown document that summarizes the key points and insights from this conversation.
    Use proper markdown formatting including:
    - Clear headers (##) for main sections
    - Code blocks (```) where appropriate
    - Bullet points for lists
    - Bold/italic for emphasis
    Start with a brief overview/introduction.
    """

    cmd = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', ?) as response
    """
    
    try:
        result = session.sql(cmd, params=[prompt]).collect()
        return result[0]["RESPONSE"]
    except Exception as e:
        st.error(f"Error generating markdown report: {str(e)}")
        return "Failed to generate report."

def main():
    """Main application entry point.

    Sets up the Streamlit interface and manages the application flow:
    - Initializes Snowflake connection
    - Handles file uploads
    - Manages chat interface
    - Provides configuration options
    - Enables report generation
    """
    st.set_page_config(page_title="Documentation Assistant", page_icon="📚")
    
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    session = init_snowflake()
    if not session:
        st.stop()
    
    st.title("📚 Documentation Assistant")
    
    # Add Export Report button in the main area above chat
    if st.session_state.messages:  # Only show if there are messages
        if st.button("📑 Export Markdown Report"):
            report = generate_markdown_report(session, st.session_state.messages)
            st.download_button(
                label="💾 Download Report",
                data=report.encode(),
                file_name="conversation_report.md",
                mime="text/markdown"
            )
    
    # Add file upload section
    st.markdown("### 📤 Upload New Documentation")
    uploaded_file = st.file_uploader("Upload a markdown file", type=['md', 'markdown'])
    if uploaded_file is not None:
        if st.button("Process File"):
            with st.spinner("Processing file..."):
                if process_uploaded_file(session, uploaded_file):
                    st.success(f"Successfully processed {uploaded_file.name}")
                    st.rerun()  # Refresh to show new content
    
    st.markdown("""
    Ask questions about your documentation. I'll help you find relevant information.
    """)
    
    with st.sidebar:
        st.header("Settings")
        num_chunks = st.slider("Number of context chunks", 1, 5, 3)
        similarity_threshold = st.slider(
            "Similarity Threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.7,
            step=0.05,
            help="Minimum similarity score required for chunks to be considered relevant"
        )
        show_context = st.checkbox("Show context", False)
        if st.button("Clear Chat"):
            st.session_state.messages = []
            st.rerun()
    
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    if prompt := st.chat_input("Ask about the documentation"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                chunks_df = get_similar_chunks(
                    session,
                    prompt,
                    num_chunks,
                    similarity_threshold
                )
                
                if not chunks_df.empty:
                    context = "\n\n".join(chunks_df["CHUNK_TEXT"].tolist())
                    response = generate_response(session, prompt, context)
                    st.markdown(response)
                    
                    with st.expander("🔍 View Retrieved Context", expanded=False):
                        st.markdown("### Retrieved Document Chunks")
                        st.markdown("---")
                        
                        for idx, row in chunks_df.iterrows():
                            with st.container():
                                st.markdown(f"#### Chunk {idx + 1}")
                                col1, col2 = st.columns([3, 1])
                                with col1:
                                    st.markdown(f"📄 **Source**: `{row['FILE_NAME']}`")
                                with col2:
                                    similarity_pct = row['SIMILARITY'] * 100
                                    st.markdown(f"🎯 **Similarity**: `{similarity_pct:.2f}%`")
                                
                                st.markdown("**Content:**")
                                st.markdown(f"""```text
{row['CHUNK_TEXT']}
```""")
                                st.markdown("---")
                    
                    st.session_state.messages.append({
                        "role": "assistant", 
                        "content": response
                    })
                else:
                    # If no relevant chunks found, query LLM without context
                    prompt = f"""You are an expert assistant helping users understand technical topics.
                    Answer the following question based on your general knowledge. Be concise and accurate.
                    If you're not confident about the answer, please say so.
                    
                    Question: {prompt}
                    
                    Answer:"""
                    
                    cmd = """
                        SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', ?) as response
                    """
                    
                    try:
                        result = session.sql(cmd, params=[prompt]).collect()
                        response = result[0]["RESPONSE"]
                        st.markdown(response)
                        st.info("Note: This response is based on the model's general knowledge as no relevant documentation was found.")
                        st.session_state.messages.append({
                            "role": "assistant", 
                            "content": response
                        })
                    except Exception as e:
                        st.error(f"Error generating response: {str(e)}")

if __name__ == "__main__":
    main()
