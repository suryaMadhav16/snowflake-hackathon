# main.py
import streamlit as st

def main():
    st.set_page_config(page_title="Snowflake RAG Chat", layout="wide")
    st.title("Welcome to Snowflake RAG Chat Demo")

    st.write("""
    This is a multi-page Streamlit application showcasing a Retrieval-Augmented
    Generation (RAG) workflow on Snowflake. Use the sidebar or the page menu to
    navigate to the **Chat** page.
    """)

if __name__ == "__main__":
    main()
