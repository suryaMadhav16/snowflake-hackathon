import streamlit as st
from components.chat.file_upload import FileUpload
from components.chat.chat_interface import ChatInterface
import datetime

def generate_markdown_report(messages: list) -> str:
    """Generate markdown report from chat messages"""
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    report = [
        f"# Chat Report\n",
        f"Generated on: {now}\n",
        "## Conversation\n"
    ]
    
    for msg in messages:
        role = msg.get("role", "unknown")
        content = msg.get("content", "")
        report.append(f"### {role.capitalize()}\n{content}\n")
        
        # Add references if any
        if references := msg.get("references"):
            report.append("\n**References:**")
            for ref in references:
                report.append(f"- {ref['file_name']}")
                report.append(f"  > {ref['excerpt']}\n")
    
    return "\n".join(report)

def show():
    """Show chat page"""
    st.header("💬 Chat Assistant")
    
    # File upload section
    with st.expander("📤 Upload Documentation", expanded=False):
        processed_files = FileUpload.render()
        
        if processed_files:
            st.subheader("Processed Files")
            for file in processed_files:
                status_icon = "✅" if file["status"] == "success" else "❌"
                st.markdown(f"{status_icon} **{file['name']}** ({file['type']})")
                if file.get("error"):
                    st.error(file["error"])
    
    # Chat interface
    ChatInterface.render()
    
    # Export button
    if st.session_state.messages:
        st.sidebar.subheader("💾 Export")
        if st.sidebar.button("Export Chat Report"):
            report = generate_markdown_report(st.session_state.messages)
            st.sidebar.download_button(
                "Download Report",
                report,
                file_name=f"chat_report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.md",
                mime="text/markdown"
            )
        
        if st.sidebar.button("Clear Chat"):
            ChatInterface.clear_chat()
            st.rerun()
