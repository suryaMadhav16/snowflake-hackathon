import os
import json
import asyncio
from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
from groq import AsyncGroq

from ..models.schemas import ProcessedPage, PageMetadata, CodeSnippet

class PageProcessor:
    """Handles the processing of crawled pages using Groq"""

    def __init__(self, api_key: str):
        self.client = AsyncGroq(api_key=api_key)

    async def process_page(
        self,
        url: str,
        markdown_content: str,
        markdown_path: str,
        images_dir: str,
        images: List[str] = [],
        parent_url: str = None
    ) -> ProcessedPage:
        """Process a single page and return structured data"""
        metadata_prompt = f"""
        Analyze this documentation page and extract the following:
        1. Title of the page
        2. Keywords (as a list)

        Content:
        {markdown_content[:2]}  # First 2000 chars for context

        Return as JSON with fields: title, keywords
        """

        print("metadata_prompt", metadata_prompt)

        metadata_response = await self.client.chat.completions.create(
            messages=[{"role": "user", "content": metadata_prompt}],
            model="llama-3.1-8b-instant",
            response_format={"type": "json_object"}
        )

        metadata_json = json.loads(metadata_response.choices[0].message.content)

        # Create page metadata
        metadata = PageMetadata(
            url=url,
            title=metadata_json['title'],
            keywords=metadata_json['keywords'],
            images=images,
            parent_url=parent_url,
            crawled_at=datetime.utcnow()
        )

        # Generate summary
        summary_prompt = f"""
        Create a comprehensive summary of this documentation page.
        Focus on the main concepts and key takeaways.

        Content:
        {markdown_content}
        """

        summary_response = await self.client.chat.completions.create(
            messages=[{"role": "user", "content": summary_prompt}],
            model="llama3-8b-8192"
        )

        summary = summary_response.choices[0].message.content

        # Extract code snippets
        code_prompt = f"""
        Extract and analyze code snippets from this documentation page.
        For each snippet provide:
        1. Programming language
        2. Brief description of what the code does
        3. Location/context in the page

        Return as JSON array with fields: language, code, description, location

        Content:
        {markdown_content}
        """

        code_response = await self.client.chat.completions.create(
            messages=[{"role": "user", "content": code_prompt}],
            model="llama3-8b-8192",
            response_format={"type": "json_object"}
        )

        code_snippets_json = json.loads(code_response.choices[0].message.content)
        code_snippets = [CodeSnippet(**snippet) for snippet in code_snippets_json['snippets']]

        # Create processed page
        processed_page = ProcessedPage(
            metadata=metadata,
            summary=summary,
            code_snippets=code_snippets,
            markdown_path=markdown_path,
            images_dir=images_dir
        )

        return processed_page

    async def save_processed_page(self, processed_page: ProcessedPage, output_dir: str):
        """Save processed page data as JSON"""
        # Create filename from URL
        filename = Path(processed_page.metadata.url).stem
        if not filename:
            filename = 'index'

        output_path = Path(output_dir) / f"{filename}.json"
        output_path.parent.mkdir(parents=True, exist_ok=True)

        # Save as JSON
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(processed_page.dict(), f, indent=2, default=str)
