import streamlit as st
import json
from snowflake.snowpark import Session

def get_snowflake_session():
    connection_parameters = {
        "account": st.secrets["snowflake"]["account"],
        "user": st.secrets["snowflake"]["user"],
        "password": st.secrets["snowflake"]["password"],
        "warehouse": "MEDIUM",
        "database": "LLM",
        "schema": "LLM.RAG",
        "role": "ACCOUNTADMIN"
    }
    return Session.builder.configs(connection_parameters).create()

# Call the fully qualified GET_RELEVANT_CHUNKS stored procedure using the schema name.
def call_get_relevant_chunks(session, user_query):
    try:
        result = session.sql("CALL LLM.RAG.GET_RELEVANT_CHUNKS(?)", (user_query,)).collect()
        if result and result[0][0]:
            chunks_json = result[0][0]
            return json.loads(chunks_json)
    except Exception as e:
        st.error(f"Error retrieving relevant chunks: {e}")
    return []

# Call the fully qualified ANSWER_QUERY stored procedure.
def call_answer_query(session, prompt):
    try:
        result = session.sql("CALL LLM.RAG.ANSWER_QUERY(?)", (prompt,)).collect()
        if result and result[0][0]:
            return result[0][0]
    except Exception as e:
        st.error(f"Error retrieving LLM response: {e}")
    return "No response generated."

# Build a prompt that combines the original query and the retrieved chunks.
def build_prompt(user_query, chunks):
    prompt = f"Question: {user_query}\n"
    if chunks:
        prompt += "\nRelevant Chunks:\n"
        for i, chunk in enumerate(chunks, start=1):
            text = chunk.get("chunk", "").strip()
            prompt += f"Chunk {i}: {text}\n"
    return prompt

# Process the user query: fetch the relevant chunks and generate an LLM response.
def process_user_query(user_query, n_chunks, threshold):
    session = get_snowflake_session()
    
    # Fetch relevant chunks using the fully qualified stored procedure.
    relevant_chunks = call_get_relevant_chunks(session, user_query)
    
    # Filter chunks based on threshold and only display those above 0.57
    filtered_chunks = [chunk for chunk in relevant_chunks if chunk.get("score", 0) >= threshold]
    filtered_chunks_display = [chunk for chunk in relevant_chunks if chunk.get("score", 0) > 0.57]
    
    selected_chunks = filtered_chunks[:n_chunks] if filtered_chunks else []
    
    # Display the retrieved chunks inside an expander for inspection.
    if filtered_chunks_display:
        with st.expander("View Relevant Chunks"):
            for idx, chunk in enumerate(filtered_chunks_display, start=1):
                chunk_text = chunk.get("chunk", "No content available").strip()
                score = chunk.get("score", "N/A")
                st.markdown(f"**Chunk {idx} (Score: {score}):**")
                st.write(chunk_text)
    else:
        st.info("No chunks found with relevance score above the display threshold (0.57)")
    
    if not selected_chunks:
        st.warning(f"No chunks found with relevance score above the given threshold ({threshold})")
        return "I cannot find any relevant information to answer your question with the current threshold. Please try lowering the threshold or rephrase your question."
    
    # Build the prompt from the original query and selected chunks.
    prompt = build_prompt(user_query, selected_chunks)
    
    # Call the fully qualified ANSWER_QUERY stored procedure.
    answer = call_answer_query(session, prompt)
    session.close()
    return answer

def main():
    st.set_page_config(page_title="LLM Chat", layout="wide")
    st.title("LLM Chat Interface")
    
    # Sidebar configuration
    n_chunks = st.sidebar.slider("Select number of relevant chunks", min_value=1, max_value=5, value=1)
    threshold = st.sidebar.slider("Select relevance threshold", min_value=0.0, max_value=1.0, value=0.75, step=0.01)
    
    # Initialize session state for conversation history.
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display conversation history with chat elements.
    for msg in st.session_state.messages:
        with st.chat_message(msg["role"]):
            st.markdown(msg["content"])
    
    # Capture user input using st.chat_input.
    user_input = st.chat_input("Enter your query")
    
    if user_input:
        # Append and display the user message.
        st.session_state.messages.append({"role": "user", "content": user_input})
        with st.chat_message("user"):
            st.markdown(user_input)
        
        st.info("Processing your query...")
        # Process the query—retrieve chunks and generate an assistant response.
        answer = process_user_query(user_input, n_chunks, threshold)
        
        # Append and display the assistant response.
        st.session_state.messages.append({"role": "assistant", "content": answer})
        with st.chat_message("assistant"):
            st.markdown(answer)
main()
