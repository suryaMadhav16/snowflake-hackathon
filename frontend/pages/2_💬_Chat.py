import streamlit as st
import logging
import sys
import asyncio
from datetime import datetime
from services.chat_service import ChatService

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

@st.cache_resource
def get_chat_service():
    return ChatService()

async def process_chat(chat_service, prompt):
    """Process chat message asynchronously"""
    try:
        with st.chat_message("user"):
            st.markdown(prompt)
            chat_service.add_message("user", prompt)
        
        with st.chat_message("assistant"):
            with st.spinner("Thinking..."):
                response = await chat_service.process_question(
                    prompt,
                    st.session_state.similarity_threshold,
                    st.session_state.num_chunks
                )
                st.markdown(response)
                chat_service.add_message("assistant", response)
                logger.info("Response displayed and added to chat history")
                
    except Exception as e:
        error_msg = f"Error processing chat: {str(e)}"
        logger.error(error_msg, exc_info=True)
        st.error("An error occurred while processing your question")

async def main():
    logger.info("Starting chat application")
    st.title("Documentation Q&A with Snowflake Cortex")
    
    # Get chat service
    chat_service = get_chat_service()
    chat_service.initialize_state()
    
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
            new_num_chunks = st.number_input(
                "Number of chunks to retrieve",
                min_value=1,
                max_value=10,
                value=st.session_state.num_chunks
            )
            chat_service.update_settings(new_threshold, new_num_chunks)
    except Exception as e:
        logger.error(f"Error in sidebar controls: {str(e)}", exc_info=True)
    
    # Display chat history
    try:
        for message in chat_service.get_messages():
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
    except Exception as e:
        logger.error(f"Error displaying chat history: {str(e)}", exc_info=True)
    
    # Chat input
    if prompt := st.chat_input("Ask about the documentation..."):
        logger.info(f"New question received: {prompt[:50]}...")
        await process_chat(chat_service, prompt)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.critical(f"Critical application error: {str(e)}", exc_info=True)
        st.error("A critical error occurred. Please check the logs for details.")