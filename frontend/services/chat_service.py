import logging
from typing import Dict, List, Optional
import pandas as pd
import streamlit as st
from services.snowflake_client import SnowflakeClient

logger = logging.getLogger(__name__)

class ChatService:
    def __init__(self, snowflake_client: Optional[SnowflakeClient] = None):
        self.snowflake = snowflake_client or SnowflakeClient()

    def add_message(self, role: str, content: str):
        """Add a message to the chat history"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
        st.session_state.messages.append({"role": role, "content": content})

    def get_messages(self) -> List[Dict]:
        """Get chat history"""
        return st.session_state.messages if hasattr(st.session_state, "messages") else []

    def initialize_state(self):
        """Initialize chat service state"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "similarity_threshold" not in st.session_state:
            st.session_state.similarity_threshold = 0.7
        if "num_chunks" not in st.session_state:
            st.session_state.num_chunks = 3

    async def process_question(self, question: str, similarity_threshold: float = 0.7, num_chunks: int = 3) -> str:
        """Process a question and generate a response"""
        try:
            # Get similar chunks
            chunks = self.snowflake.similar_chunks(
                query=question,
                num_chunks=num_chunks,
                similarity_threshold=similarity_threshold
            )

            # If chunks found, combine context and generate response
            if chunks:
                context = "\n\n".join(chunk["CHUNK_TEXT"] for chunk in chunks)
                logger.info(f"Generated context from {len(chunks)} chunks")
                response = self.snowflake.generate_response(question, context)
                
                # Display chunks in expander
                with st.expander("View relevant documentation chunks"):
                    for chunk in chunks:
                        st.markdown(f"**File:** {chunk['FILE_NAME']}")
                        st.markdown(f"**Chunk {chunk['CHUNK_NUMBER']}**")
                        st.markdown(f"**Similarity:** {chunk['SIMILARITY']:.4f}")
                        st.markdown(chunk["CHUNK_TEXT"])
                        st.markdown("---")
            else:
                logger.warning("No relevant chunks found, generating response without context")
                response = self.snowflake.generate_response(question)

            return response

        except Exception as e:
            error_msg = f"Error processing question: {str(e)}"
            logger.error(error_msg, exc_info=True)
            st.error("An error occurred while processing your question")
            return "I encountered an error while generating the response."

    def update_settings(self, similarity_threshold: float, num_chunks: int):
        """Update chat settings"""
        try:
            if similarity_threshold != st.session_state.similarity_threshold:
                logger.info(f"Similarity threshold changed to {similarity_threshold}")
                st.session_state.similarity_threshold = similarity_threshold
            
            if num_chunks != st.session_state.num_chunks:
                logger.info(f"Number of chunks changed to {num_chunks}")
                st.session_state.num_chunks = num_chunks

        except Exception as e:
            logger.error(f"Error updating settings: {str(e)}", exc_info=True)

    def cleanup(self):
        """Cleanup resources"""
        try:
            self.snowflake.close()
        except Exception as e:
            logger.error(f"Error during cleanup: {str(e)}", exc_info=True)
