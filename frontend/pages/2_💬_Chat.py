import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Snowflake connection parameters
CONNECTION_PARAMETERS = {
    "account": os.getenv("SNOWFLAKE_ACCOUNT"),
    "user": os.getenv("SNOWFLAKE_USER"),
    "password": os.getenv("SNOWFLAKE_PASSWORD"),
    "warehouse": "Medium",
    "database": "LLM",
    "schema": "RAG"
}

def init_snowflake_session():
    """Initialize Snowflake session"""
    try:
        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        session.use_warehouse("Medium")
        session.use_database("LLM")
        session.use_schema("RAG")
        return session
    except Exception as e:
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

def get_similar_chunks(session, question, num_chunks=3, similarity_threshold=0.7):
    """Retrieve similar chunks using vector similarity"""
    try:
        query = """
        WITH embedded_question AS (
            SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', ?) as question_vector
        )
        SELECT
            file_name,
            chunk_number,
            chunk_text,
            VECTOR_COSINE_SIMILARITY(
                combined_chunk_vector, 
                (SELECT question_vector FROM embedded_question)
            ) AS similarity
        FROM 
            documentations_chunked_vectors
        WHERE 
            VECTOR_COSINE_SIMILARITY(
                combined_chunk_vector,
                (SELECT question_vector FROM embedded_question)
            ) >= ?
        ORDER BY 
            similarity DESC
        LIMIT ?
        """
        
        result = session.sql(query).bind([question, similarity_threshold, num_chunks]).collect()
        return pd.DataFrame(result)
    except Exception as e:
        st.error(f"Error retrieving similar chunks: {str(e)}")
        return pd.DataFrame()

def generate_response(session, question, context=None):
    """Generate response using Snowflake Cortex"""
    try:
        if context:
            prompt = f"""You are an AI assistant helping with documentation questions.
            Use the following context to answer the question:
            
            Context:
            {context}
            
            Question: {question}
            
            Answer the question based only on the provided context. If the context doesn't contain relevant information, say so."""
        else:
            prompt = f"""You are an AI assistant.
            Question: {question}
            
            Please provide a helpful response based on general knowledge since no specific documentation context was found."""
        
        query = """
        SELECT SNOWFLAKE.CORTEX.COMPLETE('mixtral-8x7b', 
            CONCAT('Answer this question: ', ?, 
                  CASE WHEN ? IS NOT NULL THEN CONCAT('\n\nUsing this context: ', ?) ELSE '' END)
        ) AS response
        """
        
        result = session.sql(query).bind([question, context, context]).collect()
        return result[0]["RESPONSE"]
    except Exception as e:
        st.error(f"Error generating response: {str(e)}")
        return "I encountered an error while generating the response."

def initialize_session_state():
    """Initialize Streamlit session state variables"""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    if "similarity_threshold" not in st.session_state:
        st.session_state.similarity_threshold = 0.7
    if "num_chunks" not in st.session_state:
        st.session_state.num_chunks = 3

def main():
    st.title("Documentation Q&A with Snowflake Cortex")
    
    # Initialize session state
    initialize_session_state()
    
    # Initialize Snowflake session
    session = init_snowflake_session()
    if not session:
        st.error("Failed to initialize Snowflake session")
        return
    
    # Sidebar controls
    with st.sidebar:
        st.header("Settings")
        st.session_state.similarity_threshold = st.slider(
            "Similarity Threshold",
            min_value=0.0,
            max_value=1.0,
            value=0.7,
            step=0.1
        )
        st.session_state.num_chunks = st.number_input(
            "Number of chunks to retrieve",
            min_value=1,
            max_value=10,
            value=3
        )
    
    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask about the documentation..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                # Get similar chunks
                chunks_df = get_similar_chunks(
                    session,
                    prompt,
                    st.session_state.num_chunks,
                    st.session_state.similarity_threshold
                )
                
                # Generate response
                if not chunks_df.empty:
                    context = "\n\n".join(chunks_df["CHUNK_TEXT"].tolist())
                    response = generate_response(session, prompt, context)
                    
                    # Display chunks in expander
                    with st.expander("View relevant documentation chunks"):
                        for _, row in chunks_df.iterrows():
                            st.markdown(f"**File:** {row['FILE_NAME']}")
                            st.markdown(f"**Chunk {row['CHUNK_NUMBER']}**")
                            st.markdown(f"**Similarity:** {row['SIMILARITY']:.4f}")
                            st.markdown(row["CHUNK_TEXT"])
                            st.markdown("---")
                else:
                    response = generate_response(session, prompt)
                
                st.markdown(response)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": response
                })

if __name__ == "__main__":
    main()
