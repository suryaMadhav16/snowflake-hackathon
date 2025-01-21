import streamlit as st
import asyncio
from ...database.db_manager import DatabaseManager
from pathlib import Path
import humanize
from datetime import datetime

def render_file_card(file: dict):
    """Render a file card in the grid"""
    with st.container():
        st.markdown(f"""
        <div style="padding: 1rem; border: 1px solid #ddd; border-radius: 8px; margin-bottom: 1rem;">
            <h4>{file['file_type'].upper()}</h4>
            <p><small>{file['url']}</small></p>
            <p>Size: {humanize.naturalsize(file['size'])}</p>
            <p>Saved: {humanize.naturaltime(datetime.fromisoformat(file['timestamp']))}</p>
        </div>
        """, unsafe_allow_html=True)

async def show_explorer():
    """Show the file explorer page"""
    st.title("Crawled Content Explorer")
    
    # Initialize database
    db = DatabaseManager()
    await db.initialize()
    
    # Get stats
    stats = await db.get_stats()
    
    # Show summary stats
    st.subheader("Storage Summary")
    if 'file_stats' in stats:
        cols = st.columns(len(stats['file_stats']))
        for i, (file_type, data) in enumerate(stats['file_stats'].items()):
            with cols[i]:
                st.metric(
                    f"{file_type.title()}s", 
                    f"{data['count']}",
                    f"{humanize.naturalsize(data['total_size'])}"
                )
    
    # File type filter
    file_type = st.selectbox(
        "Filter by Type",
        ["markdown", "pdf", "image", "screenshot", "all"],
        index=0
    )
    
    # Get files
    files = await db.get_saved_files(
        file_type=None if file_type == "all" else file_type
    )
    
    if not files:
        st.info("No files found. Start crawling to see files here.")
        return
        
    # Create two columns
    col1, col2 = st.columns([1, 2])
    
    with col1:
        st.subheader("Files")
        
        # Group files by domain
        from urllib.parse import urlparse
        files_by_domain = {}
        for file in files:
            domain = urlparse(file['url']).netloc
            if domain not in files_by_domain:
                files_by_domain[domain] = []
            files_by_domain[domain].append(file)
        
        # Show files grouped by domain
        for domain in sorted(files_by_domain.keys()):
            with st.expander(f"📁 {domain} ({len(files_by_domain[domain])} files)"):
                for file in files_by_domain[domain]:
                    if st.button(
                        f"📄 {file['file_type']}: {Path(file['file_path']).name}",
                        key=file['file_path'],
                        help=file['url']
                    ):
                        st.session_state.selected_file = file
    
    with col2:
        st.subheader("Content Preview")
        if 'selected_file' in st.session_state:
            file = st.session_state.selected_file
            
            # Show file info
            st.markdown(f"**URL:** {file['url']}")
            st.markdown(f"**Type:** {file['file_type']}")
            st.markdown(f"**Size:** {humanize.naturalsize(file['size'])}")
            
            # Show content based on type
            if file['file_type'] == 'markdown':
                content = await db.get_markdown_content(file['url'])
                if content:
                    with st.expander("Content", expanded=True):
                        st.markdown(content['content'])
                    with st.expander("Page Metadata"):
                        st.json(content['page_metadata'])
                else:
                    st.warning("Content not found or file deleted.")
                    
            elif file['file_type'] in ['image', 'screenshot']:
                try:
                    st.image(file['file_path'])
                except Exception as e:
                    st.error(f"Failed to load image: {e}")
                    
            elif file['file_type'] == 'pdf':
                st.markdown(f"[Open PDF]({file['file_path']})")
            
            # Show file metadata
            if file.get('metadata'):
                with st.expander("File Metadata"):
                    st.json(file['metadata'])

def main():
    """Main function for the explorer page"""
    st.set_page_config(
        page_title="Content Explorer",
        page_icon="🔍",
        layout="wide"
    )
    asyncio.run(show_explorer())

if __name__ == "__main__":
    main()