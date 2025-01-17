import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from urllib.parse import urlparse

logger = logging.getLogger(__name__)

class DatabaseHandler:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.conn = sqlite3.connect(str(db_path))
        self._setup_tables()

    def reset_database(self):
        cursor = self.conn.cursor()
        cursor.executescript('''
            DROP TABLE IF EXISTS pages;
            DROP TABLE IF EXISTS images;
            DROP TABLE IF EXISTS links;
            DROP TABLE IF EXISTS crawl_stats;
            DROP TABLE IF EXISTS performance_metrics;
        ''')
        self.conn.commit()
        self._setup_tables()

    def _setup_tables(self):
        cursor = self.conn.cursor()
        cursor.executescript('''
            CREATE TABLE IF NOT EXISTS pages (
                id INTEGER PRIMARY KEY,
                url TEXT UNIQUE NOT NULL,
                domain TEXT NOT NULL,
                path TEXT NOT NULL,
                title TEXT,
                filepath TEXT,
                status TEXT NOT NULL,
                error_message TEXT,
                word_count INTEGER DEFAULT 0,
                crawled_at TIMESTAMP NOT NULL,
                last_modified TIMESTAMP,
                screenshot_path TEXT CHECK (screenshot_path IS NULL OR length(screenshot_path) > 0),
                pdf_path TEXT CHECK (pdf_path IS NULL OR length(pdf_path) > 0),
                metadata JSON,
                processed BOOLEAN DEFAULT FALSE,
                hash TEXT,
                UNIQUE(url)
            );
            
            CREATE INDEX IF NOT EXISTS idx_pages_domain ON pages(domain);
            CREATE INDEX IF NOT EXISTS idx_pages_status ON pages(status);
            CREATE INDEX IF NOT EXISTS idx_pages_processed ON pages(processed);
            CREATE INDEX IF NOT EXISTS idx_pages_hash ON pages(hash);
            
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT NOT NULL,
                filepath TEXT NOT NULL,
                alt_text TEXT,
                score REAL,
                size INTEGER,
                status TEXT NOT NULL DEFAULT 'pending',
                error_message TEXT,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_images_page_id ON images(page_id);
            CREATE INDEX IF NOT EXISTS idx_images_status ON images(status);
            
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT NOT NULL,
                is_internal BOOLEAN,
                text_content TEXT,
                metadata JSON,
                discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                processed BOOLEAN DEFAULT FALSE,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_links_page_id ON links(page_id);
            CREATE INDEX IF NOT EXISTS idx_links_processed ON links(processed);
            
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_urls INTEGER DEFAULT 0,
                successful INTEGER DEFAULT 0,
                failed INTEGER DEFAULT 0,
                current_memory_usage REAL DEFAULT 0.0,
                crawler_config JSON,
                performance_metrics JSON
            );
            
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY,
                crawl_id INTEGER,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                memory_usage REAL,
                cpu_usage REAL,
                active_sessions INTEGER,
                errors_count INTEGER,
                FOREIGN KEY (crawl_id) REFERENCES crawl_stats(id)
            );
            
            CREATE INDEX IF NOT EXISTS idx_performance_crawl_id ON performance_metrics(crawl_id);
        ''')
        self.conn.commit()

    def start_crawl(self, config: Dict[str, Any] = None) -> int:
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO crawl_stats (
                start_time, total_urls, successful, failed, 
                current_memory_usage, crawler_config
            )
            VALUES (?, 0, 0, 0, 0.0, ?)
        ''', (
            datetime.utcnow(),
            json.dumps(config) if config else None
        ))
        crawl_id = cursor.lastrowid
        self.conn.commit()
        return crawl_id

    def end_crawl(self, crawl_id: int, metrics: Dict[str, Any] = None):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE crawl_stats 
            SET end_time = ?,
                performance_metrics = ?
            WHERE id = ?
        ''', (
            datetime.utcnow(),
            json.dumps(metrics) if metrics else None,
            crawl_id
        ))
        self.conn.commit()

    def save_page_metadata(self, **kwargs):
        cursor = self.conn.cursor()
        try:
            # Convert any Path objects to strings
            filepath = str(kwargs.get('filepath')) if kwargs.get('filepath') else None
            screenshot_path = str(kwargs.get('screenshot_path')) if kwargs.get('screenshot_path') else None
            pdf_path = str(kwargs.get('pdf_path')) if kwargs.get('pdf_path') else None
            url = kwargs.get('url')
            parsed_url = urlparse(url)

            cursor.execute('''
                INSERT OR REPLACE INTO pages (
                    url, domain, path, title, filepath, status, error_message, 
                    word_count, crawled_at, last_modified, screenshot_path, 
                    pdf_path, metadata, processed
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                url,
                parsed_url.netloc,
                parsed_url.path,
                kwargs.get('title'),
                filepath,
                kwargs.get('status'),
                kwargs.get('error_message'),
                kwargs.get('word_count', 0),
                datetime.utcnow(),
                kwargs.get('last_modified'),
                screenshot_path,
                pdf_path,
                json.dumps(kwargs.get('metadata')) if kwargs.get('metadata') else None,
                kwargs.get('processed', True)
            ))
            page_id = cursor.lastrowid

            if kwargs.get('images'):
                # Convert any Path objects in image filepaths
                image_data = []
                for img in kwargs['images']:
                    img_filepath = str(img.get('filepath')) if img.get('filepath') else None
                    image_data.append((
                        page_id,
                        img['url'],
                        img_filepath,
                        img.get('alt'),
                        img.get('score'),
                        img.get('size')
                    ))

                cursor.executemany('''
                    INSERT INTO images (
                        page_id, url, filepath, alt_text, 
                        score, size
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                ''', image_data)

            if kwargs.get('links'):
                # Ensure links are strings
                link_data = [
                    (page_id, str(link), True, '')
                    for link in kwargs['links']
                ]

                cursor.executemany('''
                    INSERT INTO links (
                        page_id, url, is_internal, text_content
                    )
                    VALUES (?, ?, ?, ?)
                ''', link_data)

            self.conn.commit()

        except Exception as e:
            logger.error(f"Error saving metadata for {kwargs.get('url')}: {str(e)}")
            self.conn.rollback()

    def update_crawl_stats(self, crawl_id: int, success: bool):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE crawl_stats 
            SET total_urls = total_urls + 1,
                successful = successful + CASE WHEN ? THEN 1 ELSE 0 END,
                failed = failed + CASE WHEN ? THEN 0 ELSE 1 END
            WHERE id = ?
        ''', (success, success, crawl_id))
        self.conn.commit()

    def update_memory_usage(self, crawl_id: int, memory_usage: float):
        cursor = self.conn.cursor()
        cursor.execute('''
            UPDATE crawl_stats 
            SET current_memory_usage = ?
            WHERE id = ?
        ''', (memory_usage, crawl_id))
        self.conn.commit()

    def get_crawl_stats(self, crawl_id: int) -> Dict:
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
        self.conn.close()