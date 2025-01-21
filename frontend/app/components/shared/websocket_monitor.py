import streamlit as st
import datetime
import asyncio

async def test_websocket_connection():
    """Test WebSocket connectivity"""
    if "ws_client" not in st.session_state:
        st.error("WebSocket client not initialized")
        return False
        
    return await st.session_state.ws_client.test_connection()

def render_connection_details(conn_type: str, status: dict):
    """Render detailed connection status"""
    # Connection state with color coding
    if status["connected"]:
        st.sidebar.success("🟢 Connected")
    else:
        st.sidebar.error("🔴 Disconnected")
    
    # Connection details in expandable section
    with st.sidebar.expander(f"{conn_type.title()} Details", expanded=False):
        # Connection URL
        if status.get("connection_url"):
            st.code(status["connection_url"], language="text")
        
        # Connection ID if available
        if status.get("connection_id"):
            st.text(f"Connection ID: {status['connection_id']}")
        
        # Last message time
        if status.get("last_message_time"):
            last_msg = datetime.datetime.fromtimestamp(status["last_message_time"])
            st.text(f"Last Message: {last_msg.strftime('%H:%M:%S')}")
        
        # Connected timestamp
        if status.get("connected_at"):
            connected = datetime.datetime.fromtimestamp(status["connected_at"])
            st.text(f"Connected At: {connected.strftime('%H:%M:%S')}")
            
        # Retry count
        if "retry_count" in status:
            st.text(f"Retry Attempts: {status['retry_count']}")
        
        # Error information
        if status.get("last_error"):
            st.error(f"Last Error: {status['last_error']}")

def render_websocket_status():
    """Render WebSocket connection status"""
    if "ws_client" not in st.session_state:
        st.error("WebSocket client not initialized")
        return

    st.sidebar.markdown("### 🔌 WebSocket Status")
    
    # Add refresh and test buttons
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.button("🔄 Refresh"):
            st.experimental_rerun()
    with col2:
        if st.button("🔌 Test"):
            if asyncio.run(test_websocket_connection()):
                st.success("Connection test passed!")
            else:
                st.error("Connection test failed!")
    
    # Show base WebSocket URL
    ws_client = st.session_state.ws_client
    # st.sidebar.code(ws_client.api_url, language="text")
    
    # Check connections
    metrics_status = ws_client.get_connection_status("metrics")
    progress_status = ws_client.get_connection_status("progress")
    
    # Metrics Status
    st.sidebar.markdown("#### Metrics Connection")
    render_connection_details("metrics", metrics_status)
    
    # Progress Status
    st.sidebar.markdown("#### Progress Connection")
    render_connection_details("progress", progress_status)
    
    st.sidebar.markdown("---")