import os
import logging
import base64
import mimetypes
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
from database.snowflake_manager import SnowflakeManager

logger = logging.getLogger(__name__)

class ContentProcessor:
    """A content processor for handling and storing crawled web content in Snowflake.
    
    This class manages the processing and storage of various types of web content including:
    - Markdown content
    - Images
    - PDFs
    - Screenshots
    
    The processor handles temporary file management, content encoding/decoding,
    and uploading to Snowflake stages with appropriate metadata.

    Attributes:
        domain (str): The domain being processed
        db (SnowflakeManager): Database manager for Snowflake operations
        temp_dir (Path): Temporary directory for processing content

    Example:
        >>> processor = ContentProcessor("example.com")
        >>> saved_files = await processor.save_content(crawl_result)
        >>> print(f"Saved {len(saved_files['images'])} images")
    """
    
    def __init__(self, domain: str, db: SnowflakeManager = None):
        """Initialize the content processor.

        Args:
            domain (str): The domain being processed
            db (SnowflakeManager, optional): Snowflake database manager.
                If not provided, creates a new instance.
        """
        self.domain = domain
        self.db = db or SnowflakeManager()
        
        # Temporary directory for processing before upload
        self.temp_dir = Path('/tmp/webscrapper_temp') / domain
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Initialized content processor for domain: {domain}")

    def _get_safe_filename(self, url: str, ext: str = '') -> str:
        """Generate a safe filename from a URL.

        Creates a unique, filesystem-safe filename by hashing the URL
        and appending the specified extension.

        Args:
            url (str): The URL to generate filename from
            ext (str, optional): File extension to append. Defaults to ''.

        Returns:
            str: A safe filename with the specified extension.

        Example:
            >>> processor._get_safe_filename("https://example.com/image", ".png")
            '8793224690273.png'
        """
        safe_name = str(abs(hash(url)))
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        return safe_name + ext

    def _decode_base64(self, data: str) -> Optional[bytes]:
        """Safely decode base64-encoded data.

        Handles both raw base64 strings and data URLs (with or without content type prefix).

        Args:
            data (str): The base64-encoded string to decode.

        Returns:
            Optional[bytes]: Decoded bytes if successful, None if decoding fails.

        Example:
            >>> data = processor._decode_base64("data:image/png;base64,iVBORw0...")
            >>> if data:
            ...     print(f"Decoded {len(data)} bytes")
        """
        if not data:
            return None
            
        try:
            if ',' in data:
                data = data.split(',', 1)[1]
            data = data.strip()
            return base64.b64decode(data)
        except Exception as e:
            logger.warning(f"Failed to decode base64 data: {str(e)}")
            return None

    async def save_content(self, result) -> Dict[str, List[Dict]]:
        """Save crawled content to Snowflake stage.

        Processes and stores different types of content including:
        - Markdown content (with RAG processing)
        - PDF documents
        - Images (with metadata)
        - Page screenshots

        Args:
            result: The crawl result object containing various content types.

        Returns:
            Dict[str, List[Dict]]: Dictionary containing saved file information:
                {
                    'markdown': [{'url': str, 'file_name': str, ...}],
                    'images': [{'url': str, 'file_name': str, ...}],
                    'pdf': [{'url': str, 'file_name': str, ...}],
                    'screenshot': [{'url': str, 'file_name': str, ...}]
                }

        Example:
            >>> saved = await processor.save_content(crawl_result)
            >>> for content_type, files in saved.items():
            ...     print(f"Saved {len(files)} {content_type} files")
        """
        saved_files = {
            'markdown': [],
            'images': [],
            'pdf': [],
            'screenshot': []
        }
        
        try:
            # Save markdown content
            if result.markdown_v2:
                try:
                    content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else str(result.markdown_v2)
                    filename = self._get_safe_filename(result.url, '.md')
                    temp_path = self.temp_dir / filename
                    temp_path.write_text(content, encoding='utf-8')
                    
                    # Upload to Snowflake stage
                    stage_path = f"{self.domain}/markdown/{filename}"
                    if await self.db.upload_to_stage(temp_path, stage_path):
                        file_info = {
                            'url': result.url,
                            'file_name': filename,
                            'file_type': 'markdown',
                            'size': temp_path.stat().st_size,
                            'content_type': 'text/markdown',
                            'metadata': {
                                'type': 'markdown_v2',
                                'markdown_content': content  # Store content for RAG processing
                            }
                        }
                        saved_files['markdown'].append(file_info)
                        await self.db.save_file_metadata(result.url, file_info)
                        logger.info(f"Saved markdown for {result.url}")
                    
                    # Cleanup temp file
                    temp_path.unlink()
                except Exception as e:
                    logger.error(f"Failed to save markdown for {result.url}: {e}")

            # Save PDF
            if result.pdf:
                try:
                    filename = self._get_safe_filename(result.url, '.pdf')
                    temp_path = self.temp_dir / filename
                    temp_path.write_bytes(result.pdf)
                    
                    stage_path = f"{self.domain}/pdfs/{filename}"
                    if await self.db.upload_to_stage(temp_path, stage_path):
                        file_info = {
                            'url': result.url,
                            'file_name': filename,
                            'file_type': 'pdf',
                            'size': temp_path.stat().st_size,
                            'content_type': 'application/pdf'
                        }
                        saved_files['pdf'].append(file_info)
                        await self.db.save_file_metadata(result.url, file_info)
                        logger.info(f"Saved PDF for {result.url}")
                    
                    temp_path.unlink()
                except Exception as e:
                    logger.error(f"Failed to save PDF for {result.url}: {e}")

            # Save images
            if result.media and isinstance(result.media, dict) and 'images' in result.media:
                for idx, img in enumerate(result.media['images']):
                    try:
                        if 'data' in img and img.get('src'):
                            img_data = self._decode_base64(img['data'])
                            if img_data:
                                ext = os.path.splitext(img['src'])[1] or '.png'
                                filename = self._get_safe_filename(img['src'], ext)
                                temp_path = self.temp_dir / filename
                                temp_path.write_bytes(img_data)
                                
                                stage_path = f"{self.domain}/images/{filename}"
                                content_type = mimetypes.guess_type(temp_path)[0] or 'image/unknown'
                                
                                if await self.db.upload_to_stage(temp_path, stage_path):
                                    file_info = {
                                        'url': result.url,
                                        'file_name': filename,
                                        'file_type': 'image',
                                        'size': temp_path.stat().st_size,
                                        'content_type': content_type,
                                        'metadata': {
                                            'alt': img.get('alt', ''),
                                            'score': img.get('score', 0),
                                            'src': img['src']
                                        }
                                    }
                                    saved_files['images'].append(file_info)
                                    await self.db.save_file_metadata(result.url, file_info)
                                
                                temp_path.unlink()
                    except Exception as e:
                        logger.error(f"Failed to save image {idx} from {result.url}: {e}")

            # Save screenshot
            if result.screenshot:
                try:
                    screenshot_data = self._decode_base64(result.screenshot)
                    if screenshot_data:
                        filename = self._get_safe_filename(result.url, '.png')
                        temp_path = self.temp_dir / filename
                        temp_path.write_bytes(screenshot_data)
                        
                        stage_path = f"{self.domain}/screenshots/{filename}"
                        if await self.db.upload_to_stage(temp_path, stage_path):
                            file_info = {
                                'url': result.url,
                                'file_name': filename,
                                'file_type': 'screenshot',
                                'size': temp_path.stat().st_size,
                                'content_type': 'image/png'
                            }
                            saved_files['screenshot'].append(file_info)
                            await self.db.save_file_metadata(result.url, file_info)
                            logger.info(f"Saved screenshot for {result.url}")
                        
                        temp_path.unlink()
                except Exception as e:
                    logger.error(f"Failed to save screenshot for {result.url}: {e}")

            return saved_files
            
        except Exception as e:
            logger.error(f"Error saving content for {result.url}: {e}")
            return saved_files

    async def cleanup_temp_dir(self):
        """Clean up the temporary processing directory.

        Removes all temporary files and directories created during content processing.
        Should be called after processing is complete to free up disk space.

        Example:
            >>> await processor.save_content(result)
            >>> await processor.cleanup_temp_dir()  # Clean up after processing
        """
        try:
            if self.temp_dir.exists():
                for file in self.temp_dir.glob('*'):
                    try:
                        file.unlink()
                    except Exception as e:
                        logger.error(f"Failed to delete temp file {file}: {e}")
                self.temp_dir.rmdir()
        except Exception as e:
            logger.error(f"Error cleaning up temp directory: {e}")

    async def get_storage_info(self) -> Dict:
        """Get storage usage statistics from Snowflake.

        Returns:
            Dict: Storage statistics including:
                - Total storage used
                - Storage by content type
                - File counts
                - Stage information

        Example:
            >>> stats = await processor.get_storage_info()
            >>> print(f"Total storage used: {stats.get('total_size', 0)} bytes")
        """
        try:
            return await self.db.get_stats()
        except Exception as e:
            logger.error(f"Error getting storage info: {e}")
            return {}
