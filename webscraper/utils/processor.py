import os
import logging
import base64
from pathlib import Path
from typing import Dict, List
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class ContentProcessor:
    def __init__(self, domain_dir: Path, domain: str):
        self.domain_dir = domain_dir
        self.domain = domain
        self.content_dir = domain_dir / 'content'
        self.images_dir = domain_dir / 'images'
        self.pdfs_dir = domain_dir / 'pdfs'
        
        for directory in [self.content_dir, self.images_dir, self.pdfs_dir]:
            directory.mkdir(parents=True, exist_ok=True)

    async def save_images(self, images: List[Dict], url: str) -> List[Dict]:
        saved_images = []
        for img in images:
            try:
                img_url = img.get('src', '')
                if not img_url:
                    continue
                
                if urlparse(img_url).netloc != self.domain:
                    continue
                
                ext = os.path.splitext(img_url)[1] or '.png'
                img_path = self.images_dir / f"{hash(img_url)}{ext}"
                
                if 'data' in img:
                    try:
                        img_data = base64.b64decode(img['data'])
                        img_path.write_bytes(img_data)
                        saved_images.append({
                            'url': img_url,
                            'filepath': str(img_path.relative_to(self.domain_dir)),
                            'alt': img.get('alt', ''),
                            'size': len(img_data)
                        })
                    except Exception as e:
                        logger.warning(f"Failed to decode/save image {img_url}: {str(e)}")
                
                if 'score' in img:
                    saved_images[-1]['score'] = img['score']
                
            except Exception as e:
                logger.warning(f"Error processing image: {str(e)}")
        
        return saved_images

    async def save_pdf(self, pdf_data: bytes, url: str) -> Dict:
        try:
            pdf_path = self.pdfs_dir / f"{hash(url)}.pdf"
            pdf_path.write_bytes(pdf_data)
            return {
                'url': url,
                'filepath': str(pdf_path.relative_to(self.domain_dir)),
                'size': len(pdf_data)
            }
        except Exception as e:
            logger.warning(f"Failed to save PDF for {url}: {str(e)}")
            return None

    def save_markdown(self, content: str | object, url: str) -> Path:
        try:
            filepath = self.content_dir / f"{hash(url)}.md"
            
            # Handle MarkdownGenerationResult object
            if hasattr(content, 'raw_markdown'):
                content = content.raw_markdown
            elif hasattr(content, 'content'):
                content = content.content
            elif not isinstance(content, str):
                content = str(content)
            
            # Ensure content ends with newline
            if not content.endswith('\n'):
                content += '\n'
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            return filepath.relative_to(self.domain_dir)
            
        except Exception as e:
            logger.error(f"Failed to save markdown for {url}: {str(e)}")
            raise