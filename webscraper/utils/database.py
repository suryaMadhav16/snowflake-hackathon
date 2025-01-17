import sqlite3
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any

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
                url TEXT UNIQUE,
                title TEXT,
                filepath TEXT,
                status TEXT,
                error_message TEXT,
                word_count INTEGER,
                crawled_at TIMESTAMP,
                screenshot_path TEXT,
                pdf_path TEXT,
                metadata JSON
            );
            
            CREATE TABLE IF NOT EXISTS images (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT,
                filepath TEXT,
                alt_text TEXT,
                score REAL,
                size INTEGER,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE TABLE IF NOT EXISTS links (
                id INTEGER PRIMARY KEY,
                page_id INTEGER,
                url TEXT,
                is_internal BOOLEAN,
                text_content TEXT,
                metadata JSON,
                FOREIGN KEY (page_id) REFERENCES pages(id)
            );
            
            CREATE TABLE IF NOT EXISTS crawl_stats (
                id INTEGER PRIMARY KEY,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                total_urls INTEGER,
                successful INTEGER,
                failed INTEGER,
                current_memory_usage REAL,
                crawler_config JSON,
                performance_metrics JSON
            );
            
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY,
                crawl_id INTEGER,
                timestamp TIMESTAMP,
                memory_usage REAL,
                cpu_usage REAL,
                active_sessions INTEGER,
                errors_count INTEGER,
                FOREIGN KEY (crawl_id) REFERENCES crawl_stats(id)
            );
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

            cursor.execute('''
                INSERT OR REPLACE INTO pages (
                    url, title, filepath, status, error_message, 
                    word_count, crawled_at, screenshot_path, 
                    pdf_path, metadata
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                kwargs.get('url'),
                kwargs.get('title'),
                filepath,
                kwargs.get('status'),
                kwargs.get('error_message'),
                kwargs.get('word_count', 0),
                datetime.utcnow(),
                screenshot_path,
                pdf_path,
                json.dumps(kwargs.get('metadata')) if kwargs.get('metadata') else None
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