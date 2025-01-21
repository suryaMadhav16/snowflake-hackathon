import streamlit as st
import tempfile
import os
from typing import List, Dict, Optional
from utils.snowflake import SnowflakeManager

class FileUpload:
    """Component for document file upload"""
    
    @staticmethod
    def render() -> Optional[List[Dict]]:
        """Render file upload component and process uploads"""
        uploaded_files = st.file_uploader(
            "Upload Documentation",
            accept_multiple_files=True,
            type=["md", "txt", "pdf"],
            help="Upload documentation files to process"
        )
        
        if not uploaded_files:
            return None
        
        # Initialize Snowflake manager
        snowflake = SnowflakeManager()
        processed_files = []
        
        # Process each file
        for file in uploaded_files:
            with st.spinner(f"Processing {file.name}..."):
                with tempfile.NamedTemporaryFile(delete=False) as temp:
                    try:
                        # Save file locally
                        temp.write(file.getvalue())
                        temp.flush()
                        
                        # Add metadata
                        metadata = {
                            "original_name": file.name,
                            "content_type": file.type,
                            "size": file.size
                        }
                        
                        # Process with Snowflake
                        if snowflake.process_document(temp.name, metadata):
                            st.success(f"✅ Processed {file.name}")
                            processed_files.append({
                                "name": file.name,
                                "type": file.type,
                                "size": file.size,
                                "status": "success"
                            })
                        else:
                            st.error(f"❌ Failed to process {file.name}")
                            processed_files.append({
                                "name": file.name,
                                "type": file.type,
                                "size": file.size,
                                "status": "failed"
                            })
                    except Exception as e:
                        st.error(f"❌ Error processing {file.name}: {str(e)}")
                        processed_files.append({
                            "name": file.name,
                            "type": file.type,
                            "size": file.size,
                            "status": "error",
                            "error": str(e)
                        })
                    finally:
                        # Clean up temp file
                        if os.path.exists(temp.name):
                            os.unlink(temp.name)
        
        if processed_files:
            st.success(f"Processed {len(processed_files)} files")
            return processed_files
        
        return None
