# chat_page.py
import streamlit as st
from snowflake_service import SnowflakeService

# We'll cache the service instance so we don't re-initialize for every interaction:
@st.cache_resource
def get_snowflake_service():
    # Use the same connection parameters you have in __main__:
    connection_params = {
        "account": "blcquff-dcb49840",
        "user": "TAZ16",
        "password": "Srinijani@95",
        "role": "ACCOUNTADMIN",
        "warehouse": "COMPUTE_WH",
        "database": "RAG_CHAT_DB",
        "schema": "RAG_CHAT_SCHEMA"
    }
    return SnowflakeService(connection_params)

def show_chat_page():
    st.title("Snowflake RAG Chat")

    # 1. Initialize/reuse SnowflakeService
    service = get_snowflake_service()

    # 2. List files from Snowflake stage in the sidebar:
    st.sidebar.header("Files in Stage")
    files = []
    try:
        list_stmt = f"LIST @{service.db_name}.{service.schema_name}.document_stage"
        rows = service.session.sql(list_stmt).collect()
        for row in rows:
            # Each `row` is a Snowpark Row object, convert to dict or direct access
            rowdict = row.asDict()
            file_name = rowdict.get('name', 'Unknown')
            files.append(file_name)
    except Exception as e:
        st.sidebar.error(f"Error listing stage: {e}")

    if files:
        for f in files:
            st.sidebar.write(f)
    else:
        st.sidebar.write("No files found in stage.")

    st.markdown("---")

    # 3. Chat interface:
    st.subheader("Ask a Question:")
    user_question = st.text_input("Enter your question here")

    # 4. On 'Send' button, call the RAG pipeline
    if st.button("Send"):
        if user_question.strip():
            # Call the chat method (retrieval + generation + feedback)
            answer, meta = service.chat(user_question)

            # Display the answer
            st.write("**Answer:**", answer)

            # Optionally display context or feedback
            if meta:
                st.write("---")
                st.write("**Retrieved Context:**")
                context = meta.get('context', [])
                for i, chunk in enumerate(context):
                    st.write(f"**Chunk {i+1}:** {chunk.get('CHUNK_TEXT','')[:200]}...")

                st.write("---")
                st.write("**Feedback Scores:**")
                scores = meta.get('feedback_scores', {})
                for k,v in scores.items():
                    if isinstance(v, dict) and 'score' in v:
                        st.write(f"- {k.title()}: {v['score']:.3f}")
                    else:
                        # It's just a float or something simpler
                        st.write(f"- {k.title()}: {v}")
        else:
            st.warning("Please enter a question.")

def main():
    # If you prefer the old approach of a single script that navigates pages:
    show_chat_page()

if __name__ == "__main__":
    main()
