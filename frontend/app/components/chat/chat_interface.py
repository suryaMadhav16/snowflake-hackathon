import streamlit as st
from typing import Dict, List, Optional
import uuid
from utils.snowflake import SnowflakeManager

class ChatInterface:
    """Component for chat interaction"""
    
    @staticmethod
    def initialize_chat():
        """Initialize chat session state"""
        if "messages" not in st.session_state:
            st.session_state.messages = []
        if "chat_id" not in st.session_state:
            st.session_state.chat_id = str(uuid.uuid4())
    
    @staticmethod
    def display_message(message: Dict):
        """Display a chat message"""
        role = message.get("role", "assistant")
        content = message.get("content", "")
        
        with st.chat_message(role):
            st.markdown(content)
            
            # Show document references if any
            if references := message.get("references"):
                with st.expander("📚 Document References"):
                    for ref in references:
                        st.markdown(f"- **{ref['file_name']}**")
                        st.markdown(f"  > {ref['excerpt']}")
    
    @staticmethod
    def render():
        """Render chat interface"""
        # Initialize chat
        ChatInterface.initialize_chat()
        
        # Initialize Snowflake manager
        snowflake = SnowflakeManager()
        
        # Display chat header
        st.subheader("💬 Chat Interface")
        
        # Display chat messages
        for message in st.session_state.messages:
            ChatInterface.display_message(message)
        
        # Chat input
        if prompt := st.chat_input("Ask a question about your documentation..."):
            # Add user message
            st.session_state.messages.append({
                "role": "user",
                "content": prompt
            })
            ChatInterface.display_message({
                "role": "user",
                "content": prompt
            })
            
            # Search documentation
            with st.spinner("Searching documentation..."):
                results = snowflake.search_documents(prompt)
            
            if not results:
                response = {
                    "role": "assistant",
                    "content": "I couldn't find any relevant information in the documentation. Could you please rephrase your question or try a different query?"
                }
            else:
                # Process results
                references = []
                for result in results:
                    references.append({
                        "file_name": result["file_name"],
                        "excerpt": result["contents"][:200] + "...",  # Show first 200 chars
                        "score": result.get("score", 0)
                    })
                
                # Format response
                response = {
                    "role": "assistant",
                    "content": f"Based on the documentation, here's what I found:\n\n{results[0]['contents'][:500]}...",
                    "references": references
                }
            
            # Add assistant response
            st.session_state.messages.append(response)
            ChatInterface.display_message(response)
    
    @staticmethod
    def clear_chat():
        """Clear chat history"""
        if "messages" in st.session_state:
            st.session_state.messages = []
        if "chat_id" in st.session_state:
            st.session_state.chat_id = str(uuid.uuid4())
