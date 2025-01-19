import os
import logging
import base64
from pathlib import Path
from typing import Dict, List, Union, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class ContentSaver:
    """Handles saving of crawled content to filesystem"""
    
    def __init__(self, domain: str):
        """Initialize directory structure for content"""
        self.domain = domain
        self.base_dir = Path('/tmp/webscrapper') / domain
        
        # Create directories for different content types
        self.dirs = {
            'markdown': self.base_dir / 'markdown',
            'images': self.base_dir / 'images',
            'pdfs': self.base_dir / 'pdfs',
            'screenshots': self.base_dir / 'screenshots'
        }
        
        # Ensure all directories exist
        for directory in self.dirs.values():
            directory.mkdir(parents=True, exist_ok=True)
            
        logger.info(f"Initialized content directories at {self.base_dir}")

    def _decode_base64(self, data: Union[str, bytes]) -> Optional[bytes]:
        """Safely decode base64 data"""
        if not data:
            return None
            
        try:
            if isinstance(data, str):
                # Remove data URL prefix if present
                if ',' in data:
                    data = data.split(',', 1)[1]
                # Remove any whitespace
                data = data.strip()
                return base64.b64decode(data)
            elif isinstance(data, bytes):
                return data
            else:
                logger.warning(f"Unexpected data type for base64 content: {type(data)}")
                return None
        except Exception as e:
            logger.warning(f"Failed to decode base64 data: {str(e)}")
            return None

    def _get_safe_filename(self, url: str) -> str:
        """Generate safe filename from URL"""
        return str(abs(hash(url)))

    def _save_binary(self, data: bytes, directory: Path, filename: str) -> Optional[Path]:
        """Save binary data to file"""
        if not data:
            return None
        
        try:
            filepath = directory / filename
            filepath.write_bytes(data)
            return filepath.relative_to(self.base_dir)
        except Exception as e:
            logger.error(f"Failed to save binary data to {filepath}: {e}")
            return None

    async def save_content(self, result) -> Dict[str, Union[Path, List[Path]]]:
        """Save all content from a crawl result"""
        saved_paths = {}
        try:
            # Save markdown content
            if result.markdown_v2:
                content = result.markdown_v2.raw_markdown if hasattr(result.markdown_v2, 'raw_markdown') else str(result.markdown_v2)
                filepath = self.dirs['markdown'] / f"{self._get_safe_filename(result.url)}.md"
                filepath.write_text(content, encoding='utf-8')
                saved_paths['markdown'] = filepath.relative_to(self.base_dir)
                logger.debug(f"Saved markdown to {filepath}")

            # Save PDFs
            if result.pdf:
                if isinstance(result.pdf, bytes):
                    pdf_path = self._save_binary(
                        result.pdf,
                        self.dirs['pdfs'],
                        f"{self._get_safe_filename(result.url)}.pdf"
                    )
                    if pdf_path:
                        saved_paths['pdf'] = pdf_path
                        logger.debug(f"Saved PDF to {pdf_path}")

            # Save images
            if result.media and isinstance(result.media, dict) and 'images' in result.media:
                image_paths = []
                for idx, img in enumerate(result.media['images']):
                    try:
                        if 'data' in img and img.get('src'):
                            img_data = self._decode_base64(img['data'])
                            if img_data:
                                ext = os.path.splitext(img['src'])[1] or '.png'
                                img_path = self._save_binary(
                                    img_data,
                                    self.dirs['images'],
                                    f"{self._get_safe_filename(img['src'])}{ext}"
                                )
                                if img_path:
                                    image_paths.append(img_path)
                                    logger.debug(f"Saved image {idx + 1} to {img_path}")
                    except Exception as e:
                        logger.warning(f"Failed to save image {idx + 1}: {e}")
                if image_paths:
                    saved_paths['images'] = image_paths

            # Save screenshots
            if result.screenshot:
                screenshot_data = self._decode_base64(result.screenshot)
                if screenshot_data:
                    ss_path = self._save_binary(
                        screenshot_data,
                        self.dirs['screenshots'],
                        f"{self._get_safe_filename(result.url)}.png"
                    )
                    if ss_path:
                        saved_paths['screenshot'] = ss_path
                        logger.debug(f"Saved screenshot to {ss_path}")

            return saved_paths
            
        except Exception as e:
            logger.error(f"Error saving content for {result.url}: {e}", exc_info=True)
            return saved_paths

    def get_saved_path(self, content_type: str, url: str) -> Optional[Path]:
        """Get path for saved content"""
        if content_type not in self.dirs:
            return None
            
        directory = self.dirs[content_type]
        filename = self._get_safe_filename(url)
        
        ext = {
            'markdown': '.md',
            'images': '.png',
            'pdfs': '.pdf',
            'screenshots': '.png'
        }.get(content_type)
        
        if not ext:
            return None
            
        filepath = directory / f"{filename}{ext}"
        return filepath if filepath.exists() else None