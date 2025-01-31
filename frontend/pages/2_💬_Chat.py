import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import os
from dotenv import load_dotenv
import logging
import sys
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(f'app_logs_{datetime.now().strftime("%Y%m%d")}.log')
    ]
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()
logger.info("Environment variables loaded")

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
    logger.info("Attempting to initialize Snowflake session")
    try:
        session = Session.builder.configs(CONNECTION_PARAMETERS).create()
        session.use_warehouse("Medium")
        session.use_database("LLM")
        session.use_schema("RAG")
        logger.info("Snowflake session initialized successfully")
        return session
    except Exception as e:
        logger.error(f"Failed to initialize Snowflake session: {str(e)}", exc_info=True)
        st.error(f"Error connecting to Snowflake: {str(e)}")
        return None

def get_similar_chunks(session, question, num_chunks=3, similarity_threshold=0.7):
    """Retrieve similar chunks using vector similarity"""
    logger.info(f"Retrieving similar chunks for question: {question[:50]}...")
    logger.info(f"Parameters: num_chunks={num_chunks}, similarity_threshold={similarity_threshold}")
    
    try:
        query = """
        WITH embedded_question AS (
            SELECT SNOWFLAKE.CORTEX.EMBED_TEXT_768('snowflake-arctic-embed-m-v1.5', ?) AS question_vector
        )
        SELECT
            file_name,
            chunk_number,
            combined_chunk_text AS chunk_text,
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
        
        logger.debug(f"Executing query with parameters: {[question, similarity_threshold, num_chunks]}")
        result = session.sql(query, params=[question, similarity_threshold, num_chunks]).collect()
        df = pd.DataFrame(result)
        
        if df.empty:
            logger.warning("No similar chunks found")
        else:
            logger.info(f"Retrieved {len(df)} similar chunks")
            
        return df
    except Exception as e:
        logger.error(f"Error retrieving similar chunks: {str(e)}", exc_info=True)
        st.error(f"Error retrieving similar chunks: {str(e)}")
        return pd.DataFrame()

def generate_response(session, question, context=None):
    """Generate response using Snowflake Cortex"""
    logger.info("Generating response")
    logger.debug(f"Question: {question}")
    logger.debug(f"Context provided: {'Yes' if context else 'No'}")
    
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
        
        logger.debug("Executing Cortex query")
        result = session.sql(query, params=[question, context, context]).collect()
        response = result[0]["RESPONSE"]
        logger.info("Response generated successfully")
        return response
    except Exception as e:
        logger.error(f"Error generating response: {str(e)}", exc_info=True)
        return "I encountered an error while generating the response."

def initialize_session_state():
    """Initialize Streamlit session state variables"""
    logger.info("Initializing session state")
    try:
        if "messages" not in st.session_state:
            st.session_state.messages = []
            logger.debug("Initialized messages in session state")
        if "similarity_threshold" not in st.session_state:
            st.session_state.similarity_threshold = 0.7
            logger.debug("Initialized similarity threshold in session state")
        if "num_chunks" not in st.session_state:
            st.session_state.num_chunks = 3
            logger.debug("Initialized num_chunks in session state")
    except Exception as e:
        logger.error(f"Error initializing session state: {str(e)}", exc_info=True)

def main():
    logger.info("Application started")
    st.title("Documentation Q&A with Snowflake Cortex")
    
    # Initialize session state
    initialize_session_state()
    
    # Initialize Snowflake session
    session = init_snowflake_session()
    if not session:
        logger.error("Failed to initialize Snowflake session")
        return
    
    # Sidebar controls
    try:
        with st.sidebar:
            st.header("Settings")
            new_threshold = st.slider(
                "Similarity Threshold",
                min_value=0.0,
                max_value=1.0,
                value=st.session_state.similarity_threshold,
                step=0.1
            )
            if new_threshold != st.session_state.similarity_threshold:
                logger.info(f"Similarity threshold changed to {new_threshold}")
                st.session_state.similarity_threshold = new_threshold
            
            new_num_chunks = st.number_input(
                "Number of chunks to retrieve",
                min_value=1,
                max_value=10,
                value=st.session_state.num_chunks
            )
            if new_num_chunks != st.session_state.num_chunks:
                logger.info(f"Number of chunks changed to {new_num_chunks}")
                st.session_state.num_chunks = new_num_chunks
    except Exception as e:
        logger.error(f"Error in sidebar controls: {str(e)}", exc_info=True)
    
    # Display chat history
    try:
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    except Exception as e:
        logger.error(f"Error displaying chat history: {str(e)}", exc_info=True)
    
    # Chat input
    if prompt := st.chat_input("Ask about the documentation..."):
        logger.info(f"New question received: {prompt[:50]}...")
        
        try:
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
                        logger.info(f"Generated context from {len(chunks_df)} chunks")
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
                        logger.warning("No relevant chunks found, generating response without context")
                        response = generate_response(session, prompt)
                    
                    st.markdown(response)
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": response
                    })
                    logger.info("Response displayed and added to chat history")
        except Exception as e:
            logger.error(f"Error processing question: {str(e)}", exc_info=True)
            st.error("An error occurred while processing your question")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logger.critical(f"Critical application error: {str(e)}", exc_info=True)
        st.error("A critical error occurred. Please check the logs for details.")
