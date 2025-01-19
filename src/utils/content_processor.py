import os
import logging
import base64
import mimetypes
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse
from ..database.db_manager import DatabaseManager

logger = logging.getLogger(__name__)

class ContentProcessor:
    """Processes and saves crawled content to filesystem"""
    
    def __init__(self, domain: str, db: DatabaseManager = None):
        """Initialize the content processor"""
        self.domain = domain
        self.base_dir = Path('/tmp/webscrapper') / domain
        self.db = db or DatabaseManager()
        
        # Create directories for different content types
        self.dirs = {
            'markdown': self.base_dir / 'markdown',
            'images': self.base_dir / 'images',
            'pdfs': self.base_dir / 'pdfs',
            'screenshots': self.base_dir / 'screenshots'
        }
        
        # Create directories
        for directory in self.dirs.values():
            directory.mkdir(parents=True, exist_ok=True)
            
        logger.info(f"Initialized content directories at {self.base_dir}")

    def _get_safe_filename(self, url: str, ext: str = '') -> str:
        """Generate safe filename from URL"""
        safe_name = str(abs(hash(url)))
        if ext and not ext.startswith('.'):
            ext = '.' + ext
        return safe_name + ext

    def _decode_base64(self, data: str) -> Optional[bytes]:
        """Safely decode base64 data"""
        if not data:
            return None
            
        try:
            # Remove data URL prefix if present
            if ',' in data:
                data = data.split(',', 1)[1]
            # Remove whitespace
            data = data.strip()
            return base64.b64decode(data)
        except Exception as e:
            logger.warning(f"Failed to decode base64 data: {str(e)}")
            return None

    async def save_content(self, result) -> Dict[str, List[Dict]]:
        """Save all content from a crawl result"""
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
                    filepath = self.dirs['markdown'] / self._get_safe_filename(result.url, '.md')
                    filepath.write_text(content, encoding='utf-8')
                    
                    file_info = {
                        'url': result.url,
                        'file_path': str(filepath),
                        'size': filepath.stat().st_size,
                        'content_type': 'text/markdown',
                        'metadata': {'type': 'markdown_v2'}
                    }
                    saved_files['markdown'].append(file_info)
                    await self.db.save_file_path(
                        result.url, 
                        'markdown',
                        filepath,
                        'text/markdown',
                        {'type': 'markdown_v2'}
                    )
                    logger.info(f"Saved markdown for {result.url}")
                except Exception as e:
                    logger.error(f"Failed to save markdown for {result.url}: {e}")

            # Save PDF
            if result.pdf:
                try:
                    filepath = self.dirs['pdfs'] / self._get_safe_filename(result.url, '.pdf')
                    filepath.write_bytes(result.pdf)
                    
                    file_info = {
                        'url': result.url,
                        'file_path': str(filepath),
                        'size': filepath.stat().st_size,
                        'content_type': 'application/pdf'
                    }
                    saved_files['pdf'].append(file_info)
                    await self.db.save_file_path(
                        result.url,
                        'pdf',
                        filepath,
                        'application/pdf'
                    )
                    logger.info(f"Saved PDF for {result.url}")
                except Exception as e:
                    logger.error(f"Failed to save PDF for {result.url}: {e}")

            # Save images
            if result.media and isinstance(result.media, dict) and 'images' in result.media:
                for idx, img in enumerate(result.media['images']):
                    try:
                        if 'data' in img and img.get('src'):
                            img_data = self._decode_base64(img['data'])
                            if img_data:
                                # Get extension from src or default to .png
                                ext = os.path.splitext(img['src'])[1] or '.png'
                                filepath = self.dirs['images'] / self._get_safe_filename(img['src'], ext)
                                filepath.write_bytes(img_data)
                                
                                content_type = mimetypes.guess_type(filepath)[0] or 'image/unknown'
                                file_info = {
                                    'url': result.url,
                                    'image_url': img['src'],
                                    'file_path': str(filepath),
                                    'size': filepath.stat().st_size,
                                    'content_type': content_type,
                                    'metadata': {'alt': img.get('alt', ''), 'score': img.get('score', 0)}
                                }
                                saved_files['images'].append(file_info)
                                await self.db.save_file_path(
                                    result.url,
                                    'image',
                                    filepath,
                                    content_type,
                                    {'alt': img.get('alt', ''), 'score': img.get('score', 0)}
                                )
                    except Exception as e:
                        logger.error(f"Failed to save image {idx} from {result.url}: {e}")

            # Save screenshot
            if result.screenshot:
                try:
                    screenshot_data = self._decode_base64(result.screenshot)
                    if screenshot_data:
                        filepath = self.dirs['screenshots'] / self._get_safe_filename(result.url, '.png')
                        filepath.write_bytes(screenshot_data)
                        
                        file_info = {
                            'url': result.url,
                            'file_path': str(filepath),
                            'size': filepath.stat().st_size,
                            'content_type': 'image/png'
                        }
                        saved_files['screenshot'].append(file_info)
                        await self.db.save_file_path(
                            result.url,
                            'screenshot',
                            filepath,
                            'image/png'
                        )
                        logger.info(f"Saved screenshot for {result.url}")
                except Exception as e:
                    logger.error(f"Failed to save screenshot for {result.url}: {e}")

            return saved_files
            
        except Exception as e:
            logger.error(f"Error saving content for {result.url}: {e}")
            return saved_files

    async def cleanup_old_files(self, days: int = 30):
        """Clean up files older than specified days"""
        try:
            for directory in self.dirs.values():
                for file in directory.glob('*'):
                    if file.stat().st_mtime < (time.time() - days * 86400):
                        try:
                            file.unlink()
                            logger.info(f"Deleted old file: {file}")
                        except Exception as e:
                            logger.error(f"Failed to delete {file}: {e}")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def get_storage_info(self) -> Dict:
        """Get storage usage information"""
        info = {}
        try:
            total_size = 0
            for dir_type, directory in self.dirs.items():
                size = sum(f.stat().st_size for f in directory.glob('**/*') if f.is_file())
                count = sum(1 for _ in directory.glob('**/*') if f.is_file())
                info[dir_type] = {
                    'size': size,
                    'count': count
                }
                total_size += size
            info['total_size'] = total_size
        except Exception as e:
            logger.error(f"Error getting storage info: {e}")
        return info