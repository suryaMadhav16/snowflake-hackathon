import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from typing import Dict, List, Optional
import base64

class ResultsViewer:
    """Component for viewing crawl results"""
    
    def __init__(self, task_id: str):
        """Initialize results viewer"""
        self.task_id = task_id
        self.results_container = st.empty()
        self.files_container = st.empty()
        self.graph_container = st.empty()
    
    def show_discovery_results(self, results: Dict):
        """Display URL discovery results"""
        with self.results_container:
            st.subheader("🔍 Discovered URLs")
            
            if not results.get("discovered_urls"):
                st.info("No URLs discovered yet")
                return
            
            urls = results["discovered_urls"]
            df = pd.DataFrame({"URL": urls})
            
            # Show statistics
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total URLs", len(urls))
            with col2:
                st.metric("Max Depth", results.get("max_depth", 0))
            with col3:
                st.metric("Root Domain", results.get("start_url", "").split("/")[2])
            
            # Show URL table
            st.dataframe(
                df,
                use_container_width=True,
                column_config={
                    "URL": st.column_config.LinkColumn("URL")
                }
            )
    
    def show_crawl_results(
        self,
        url: str,
        results: Dict,
        files: Optional[List[Dict]] = None
    ):
        """Display crawl results for a URL"""
        with self.results_container:
            st.subheader("📄 Crawl Results")
            
            # Show basic info
            st.markdown(f"**URL:** [{url}]({url})")
            st.markdown(f"**Status:** {'✅ Success' if results.get('success') else '❌ Failed'}")
            if results.get('error_message'):
                st.error(results['error_message'])
            
            # Show content tabs
            if results.get('success'):
                tab1, tab2, tab3 = st.tabs(["Content", "Links", "Files"])
                
                with tab1:
                    if results.get('cleaned_html'):
                        st.code(results['cleaned_html'], language='html')
                    else:
                        st.info("No cleaned content available")
                
                with tab2:
                    links = results.get('links_data', {})
                    if links:
                        # Create DataFrame from links
                        links_df = []
                        for link_type, urls in links.items():
                            for url in urls:
                                links_df.append({
                                    "Type": link_type,
                                    "URL": url
                                })
                        links_df = pd.DataFrame(links_df)
                        
                        st.dataframe(
                            links_df,
                            use_container_width=True,
                            column_config={
                                "URL": st.column_config.LinkColumn("URL")
                            }
                        )
                    else:
                        st.info("No links found")
                
                with tab3:
                    if files:
                        self.show_files(files)
                    else:
                        st.info("No files saved")
    
    def show_files(self, files: List[Dict]):
        """Display saved files"""
        with self.files_container:
            # Group files by type
            file_groups = {}
            for file in files:
                file_type = file.get('file_type', 'other')
                if file_type not in file_groups:
                    file_groups[file_type] = []
                file_groups[file_type].append(file)
            
            # Create tabs for each file type
            if file_groups:
                tabs = st.tabs(list(file_groups.keys()))
                for tab, (file_type, type_files) in zip(tabs, file_groups.items()):
                    with tab:
                        for file in type_files:
                            with st.expander(f"{file_type.upper()}: {file.get('url', 'Unknown')}"):
                                # Show file info
                                st.json(file.get('metadata', {}))
                                
                                # Show preview/download based on type
                                if file_type == 'markdown':
                                    st.markdown(file.get('content', ''))
                                elif file_type in ['screenshot', 'image']:
                                    if file.get('content'):
                                        try:
                                            img_data = base64.b64decode(file['content'])
                                            st.image(img_data)
                                        except Exception as e:
                                            st.error(f"Error displaying image: {str(e)}")
                                elif file_type == 'pdf':
                                    if file.get('content'):
                                        try:
                                            pdf_data = base64.b64decode(file['content'])
                                            st.download_button(
                                                "Download PDF",
                                                pdf_data,
                                                file_name=f"{file.get('url', 'document')}.pdf",
                                                mime="application/pdf"
                                            )
                                        except Exception as e:
                                            st.error(f"Error with PDF: {str(e)}")
            else:
                st.info("No files available")
    
    def show_graph(self, graph_data: Dict):
        """Display URL discovery graph"""
        with self.graph_container:
            st.subheader("🕸️ URL Graph")
            
            if not graph_data.get('nodes') or not graph_data.get('links'):
                st.info("No graph data available")
                return
            
            # Create network graph
            nodes = graph_data['nodes']
            links = graph_data['links']
            
            # Create Plotly figure
            node_x = []
            node_y = []
            node_text = []
            edge_x = []
            edge_y = []
            
            # Create node positions using Plotly's layout algorithm
            pos = {node['id']: [i, node['depth']] for i, node in enumerate(nodes)}
            
            # Add nodes
            for node in nodes:
                x, y = pos[node['id']]
                node_x.append(x)
                node_y.append(y)
                node_text.append(node['id'])
            
            # Add edges
            for link in links:
                x0, y0 = pos[link['source']]
                x1, y1 = pos[link['target']]
                edge_x.extend([x0, x1, None])
                edge_y.extend([y0, y1, None])
            
            # Create edge trace
            edge_trace = go.Scatter(
                x=edge_x, y=edge_y,
                line=dict(width=0.5, color='#888'),
                hoverinfo='none',
                mode='lines')
            
            # Create node trace
            node_trace = go.Scatter(
                x=node_x, y=node_y,
                mode='markers+text',
                hoverinfo='text',
                text=node_text,
                textposition="top center",
                marker=dict(
                    showscale=True,
                    colorscale='YlGnBu',
                    size=10,
                    colorbar=dict(
                        thickness=15,
                        title='Node Connections',
                        xanchor='left',
                        titleside='right'
                    )
                ))
            
            # Create figure
            fig = go.Figure(data=[edge_trace, node_trace],
                          layout=go.Layout(
                              showlegend=False,
                              hovermode='closest',
                              margin=dict(b=20,l=5,r=5,t=40),
                              xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                              yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                          )
            
            # Show plot
            st.plotly_chart(fig, use_container_width=True)
    
    def cleanup(self):
        """Clean up containers"""
        self.results_container.empty()
        self.files_container.empty()
        self.graph_container.empty()
