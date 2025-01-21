import streamlit as st
from typing import List, Dict
from urllib.parse import urlparse
import json

class URLTreeVisualizer:
    """Visualizes discovered URLs in a tree structure"""
    
    def __init__(self):
        pass
    
    def _build_tree(self, urls: List[str]) -> Dict:
        """Build tree structure from URLs"""
        tree = {'name': 'root', 'children': {}}
        
        for url in sorted(urls):
            parsed = urlparse(url)
            
            # Split path into components
            path_parts = [p for p in parsed.path.split('/') if p]
            
            # Start from domain
            current = tree['children']
            domain = parsed.netloc
            
            # Add domain if not exists
            if domain not in current:
                current[domain] = {
                    'name': domain,
                    'type': 'domain',
                    'count': 0,
                    'children': {}
                }
            current[domain]['count'] += 1
            
            # Add path components
            current = current[domain]['children']
            for part in path_parts:
                if part not in current:
                    current[part] = {
                        'name': part,
                        'type': 'path',
                        'count': 0,
                        'children': {}
                    }
                current[part]['count'] += 1
                current = current[part]['children']
        
        return tree
    
    def _convert_for_vis(self, node: Dict, name: str) -> Dict:
        """Convert tree for visualization"""
        result = {
            'name': name,
            'children': []
        }
        
        if 'children' in node:
            for child_name, child in sorted(node['children'].items()):
                count = child.get('count', 0)
                child_node = self._convert_for_vis(child, f"{child_name} ({count})")
                result['children'].append(child_node)
        
        return result
    
    def render_url_tree(self, urls: List[str], graph_data: Dict = None):
        """Render URL tree visualization"""
        if not urls:
            st.warning("No URLs to display")
            return
            
        st.subheader("URL Structure")
        
        # Build tree structure
        tree = self._build_tree(urls)
        vis_tree = self._convert_for_vis(tree, 'URLs')
        
        # Create collapsible JSON tree
        with st.expander("URL Tree Structure", expanded=True):
            # Custom tree visualization using Unicode characters
            def print_tree(node, prefix="", is_last=True):
                # Prepare the line prefix
                line_prefix = "└── " if is_last else "├── "
                child_prefix = "    " if is_last else "│   "
                
                # Display current node
                st.markdown(f"```\n{prefix}{line_prefix}{node['name']}\n```")
                
                # Process children
                if 'children' in node:
                    children = node['children']
                    last_idx = len(children) - 1
                    for i, child in enumerate(children):
                        print_tree(child, prefix + child_prefix, i == last_idx)
            
            print_tree(vis_tree)
        
        # Show summary statistics
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total URLs", len(urls))
        with col2:
            unique_domains = len(set(urlparse(url).netloc for url in urls))
            st.metric("Unique Domains", unique_domains)
        with col3:
            avg_depth = sum(len([p for p in urlparse(url).path.split('/') if p]) for url in urls) / len(urls)
            st.metric("Average Depth", f"{avg_depth:.1f}")
        
        # Show details table
        with st.expander("URL Details", expanded=False):
            data = []
            for url in sorted(urls):
                parsed = urlparse(url)
                data.append({
                    'Domain': parsed.netloc,
                    'Path': parsed.path or '/',
                    'Full URL': url
                })
            st.dataframe(data)