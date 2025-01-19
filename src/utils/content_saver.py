import os
import logging
import base64
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class ContentSaver:
    """Handles saving of various content types from crawler results"""
    
    def __init__(self, domain: str):
        """Initialize directory structure for content saving"""
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

    def save_markdown(self, content: str, url: str) -> Optional[Path]:
        """Save markdown content to file"""
        try:
            # Handle different content types
            if hasattr(content, 'raw_markdown'):
                content = content.raw_markdown
            elif hasattr(content, 'content'):
                content = content.content
            elif not isinstance(content, str):
                content = str(content)
                
            # Generate unique filename
            filename = f"{hash(url)}.md"
            filepath = self.dirs['markdown'] / filename
            
            # Ensure content ends with newline
            if not content.endswith('\n'):
                content += '\n'
            
            # Write content
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
                
            logger.info(f"Saved markdown for {url} to {filepath}")
            return filepath.relative_to(self.base_dir)
            
        except Exception as e:
            logger.error(f"Failed to save markdown for {url}: {str(e)}")
            return None

    async def save_images(self, images: List[Dict], url: str) -> List[Dict]:
        """Save images from crawl results"""
        saved_images = []
        
        for img in images:
            try:
                img_url = img.get('src', '')
                if not img_url:
                    continue
                
                # Generate filename from URL
                ext = os.path.splitext(img_url)[1] or '.png'
                img_path = self.dirs['images'] / f"{hash(img_url)}{ext}"
                
                # Handle base64 encoded images
                if 'data' in img:
                    try:
                        img_data = base64.b64decode(img['data'])
                        img_path.write_bytes(img_data)
                        
                        saved_images.append({
                            'url': img_url,
                            'filepath': str(img_path.relative_to(self.base_dir)),
                            'alt': img.get('alt', ''),
                            'size': len(img_data)
                        })
                        
                        logger.info(f"Saved image from {url} to {img_path}")
                        
                    except Exception as e:
                        logger.warning(f"Failed to decode/save image {img_url}: {str(e)}")
                        
            except Exception as e:
                logger.error(f"Error processing image from {url}: {str(e)}")
                
        return saved_images

    def save_pdf(self, pdf_data: bytes, url: str) -> Optional[Dict]:
        """Save PDF content"""
        try:
            filepath = self.dirs['pdfs'] / f"{hash(url)}.pdf"
            filepath.write_bytes(pdf_data)
            
            result = {
                'url': url,
                'filepath': str(filepath.relative_to(self.base_dir)),
                'size': len(pdf_data)
            }
            
            logger.info(f"Saved PDF for {url} to {filepath}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to save PDF for {url}: {str(e)}")
            return None

    def save_screenshot(self, screenshot_data: str, url: str) -> Optional[Path]:
        """Save screenshot (base64 encoded)"""
        try:
            if not screenshot_data:
                return None
                
            filepath = self.dirs['screenshots'] / f"{hash(url)}.png"
            
            # Decode base64 data
            if ',' in screenshot_data:
                # Handle data URLs
                screenshot_data = screenshot_data.split(',', 1)[1]
                
            image_data = base64.b64decode(screenshot_data)
            filepath.write_bytes(image_data)
            
            logger.info(f"Saved screenshot for {url} to {filepath}")
            return filepath.relative_to(self.base_dir)
            
        except Exception as e:
            logger.error(f"Failed to save screenshot for {url}: {str(e)}")
            return None
