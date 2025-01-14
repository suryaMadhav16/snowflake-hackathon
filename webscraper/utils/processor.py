import os
import logging
import aiohttp
from pathlib import Path
from typing import Dict, List, Tuple
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

logger = logging.getLogger(__name__)

class ContentProcessor:
    """Process and save crawled content"""
    
    def __init__(self, domain_dir: Path, domain: str):
        self.domain_dir = domain_dir
        self.domain = domain
        self.content_dir = domain_dir / 'content'
        self.images_dir = domain_dir / 'images'
        self.pdfs_dir = domain_dir / 'pdfs'

    async def download_file(self, url: str, output_path: Path) -> bool:
        """Download a file from URL"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    if response.status == 200:
                        content = await response.read()
                        output_path.parent.mkdir(parents=True, exist_ok=True)
                        with open(output_path, 'wb') as f:
                            f.write(content)
                        return True
            return False
        except Exception as e:
            logger.warning(f"Failed to download {url}: {str(e)}")
            return False

    async def process_images(self, html: str, base_url: str) -> List[Dict]:
        """Process and download images from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []

        for img in soup.find_all('img', src=True):
            img_url = urljoin(base_url, img['src'])
            
            # Skip external images
            if urlparse(img_url).netloc != self.domain:
                continue

            img_path = self.images_dir / f"{hash(img_url)}{os.path.splitext(img_url)[1]}"
            if await self.download_file(img_url, img_path):
                images.append({
                    'url': img_url,
                    'filepath': str(img_path.relative_to(self.domain_dir))
                })

        return images

    async def process_pdfs(self, html: str, base_url: str) -> List[Dict]:
        """Process and download PDFs from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        pdfs = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            if href.lower().endswith('.pdf'):
                pdf_url = urljoin(base_url, href)
                
                # Skip external PDFs
                if urlparse(pdf_url).netloc != self.domain:
                    continue

                pdf_path = self.pdfs_dir / f"{hash(pdf_url)}.pdf"
                if await self.download_file(pdf_url, pdf_path):
                    pdfs.append({
                        'url': pdf_url,
                        'filepath': str(pdf_path.relative_to(self.domain_dir))
                    })

        return pdfs

    def extract_links(self, html: str, base_url: str) -> Tuple[List[str], List[str]]:
        """Extract internal and external links from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        internal_links = []
        external_links = []

        for link in soup.find_all('a', href=True):
            href = link['href']
            absolute_url = urljoin(base_url, href)
            
            if urlparse(absolute_url).netloc == self.domain:
                internal_links.append(absolute_url)
            else:
                external_links.append(absolute_url)

        return internal_links, external_links

    def save_markdown(self, content: str, url: str) -> Path:
        """Save markdown content to file"""
        filepath = self.content_dir / f"{hash(url)}.md"
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        
        return filepath.relative_to(self.domain_dir)

    def extract_title(self, html: str, url: str) -> str:
        """Extract title from HTML"""
        try:
            soup = BeautifulSoup(html, 'html.parser')
            return soup.title.string if soup.title else url
        except:
            return url
