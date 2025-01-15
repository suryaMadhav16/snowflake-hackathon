import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

logger = logging.getLogger(__name__)

class DatabaseHandler:
    """Handle database operations for the scraper"""
    
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self._setup_tables()

    def _setup_tables(self):
        """Initialize database tables"""
        cursor = self.conn.cursor()

        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE,
                title TEXT,
                filepath TEXT,
                status TEXT,
                error_message TEXT,
                crawled_at TIMESTAMP
            );
            
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT,
                filepath TEXT,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT,
                is_internal BOOLEAN,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_urls INTEGER,
                successful INTEGER,
                failed INTEGER,
                current_memory_usage REAL
            );
        ''')
        self.conn.commit()

    def start_crawl(self) -> int:
        """Record crawl start and return crawl_id"""
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO crawl_stats (start_time, total_urls, successful, failed, current_memory_usage)
            VALUES (?, 0, 0, 0, 0.0)
        ''', (datetime.utcnow(),))
        crawl_id = cursor.lastrowid
        self.conn.commit()
        return crawl_id

    def end_crawl(self, crawl_id: int):
        """Record crawl completion"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE crawl_stats 
            SET end_time = ?
            WHERE id = ?
        ''', (datetime.utcnow(), crawl_id))
        self.conn.commit()

    def save_page_metadata(
        self,
        url: str,
        title: str,
        filepath: str,
        images: List[Dict],
        links: List[str],
        status: str = 'success',
        error_message: Optional[str] = None
    ):
        """Save page metadata to database"""
        cursor = self.conn.cursor()
        
        try:
            # Insert page data
            cursor.execute('''
                INSERT OR REPLACE INTO pages (url, title, filepath, status, error_message, crawled_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (url, title, str(filepath), status, error_message, datetime.utcnow()))
            page_id = cursor.lastrowid
            
            # Insert images
            if images:
                cursor.executemany('''
                    INSERT INTO images (page_id, url, filepath)
                    VALUES (?, ?, ?)
                ''', [(page_id, img['url'], img.get('filepath')) for img in images])
            
            # Insert links
            if links:
                cursor.executemany('''
                    INSERT INTO links (page_id, url, is_internal)
                    VALUES (?, ?, ?)
                ''', [(page_id, link, True) for link in links])
            
            self.conn.commit()
        except Exception as e:
            logger.error(f"Error saving metadata for {url}: {str(e)}")
            self.conn.rollback()

    def update_crawl_stats(self, crawl_id: int, success: bool):
        """Update crawl statistics"""
        cursor = self.conn.cursor()
        if success:
            cursor.execute('''
                UPDATE crawl_stats 
                SET total_urls = total_urls + 1,
                    successful = successful + 1
                WHERE id = ?
            ''', (crawl_id,))
        else:
            cursor.execute('''
                UPDATE crawl_stats 
                SET total_urls = total_urls + 1,
                    failed = failed + 1
                WHERE id = ?
            ''', (crawl_id,))
        self.conn.commit()

    def update_memory_usage(self, crawl_id: int, memory_usage: float):
        """Update current memory usage"""
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE crawl_stats 
            SET current_memory_usage = ?
            WHERE id = ?
        ''', (memory_usage, crawl_id))
        self.conn.commit()

    def get_crawl_stats(self, crawl_id: int) -> Dict:
        """Get statistics for a crawl"""
        cursor = self.conn.cursor()
        cursor.execute('''
            SELECT 
                total_urls, successful, failed, 
                start_time, end_time, current_memory_usage 
            FROM crawl_stats 
            WHERE id = ?
        ''', (crawl_id,))
        
        row = cursor.fetchone()
        if row:
            return {
                'total_urls': row[0],
                'successful': row[1],
                'failed': row[2],
                'start_time': row[3],
                'end_time': row[4],
                'current_memory_usage': row[5]
            }
        return {}

    def close(self):
        """Close database connection"""
        self.conn.close()
