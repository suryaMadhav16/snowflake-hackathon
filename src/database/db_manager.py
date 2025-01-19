import sqlite3
import json
import logging
import asyncio
import aiosqlite
from typing import Dict, List, Set, Optional
from datetime import datetime
from pathlib import Path
from crawl4ai import CrawlResult

logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database operations for crawler results"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or Path.home() / '.webcrawler' / 'crawler.db'
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.url_cache: Set[str] = set()
        self._initialized = False
        self._lock = asyncio.Lock()
    
    async def initialize(self):
        """Initialize database and cache"""
        if not self._initialized:
            async with self._lock:
                if not self._initialized:
                    try:
                        await self._create_tables()
                        
                        # Load URL cache
                        cached_urls = await self.get_cached_urls()
                        self.url_cache = set(cached_urls)
                        
                        self._initialized = True
                        logger.info("Database initialized successfully")
                        
                    except Exception as e:
                        logger.error(f"Database initialization failed: {str(e)}")
                        raise
    
    async def _create_tables(self):
        """Create necessary database tables"""
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS crawl_results (
                    url TEXT PRIMARY KEY,
                    success BOOLEAN NOT NULL,
                    html TEXT,
                    cleaned_html TEXT,
                    error_message TEXT,
                    media_data TEXT,
                    links_data TEXT,
                    metadata TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            await db.execute('''
                CREATE TABLE IF NOT EXISTS crawl_stats (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    start_time DATETIME,
                    end_time DATETIME,
                    total_urls INTEGER DEFAULT 0,
                    successful_urls INTEGER DEFAULT 0,
                    failed_urls INTEGER DEFAULT 0,
                    metadata TEXT
                )
            ''')
            await db.commit()
    
    async def get_cached_urls(self) -> List[str]:
        """Get list of all cached URLs"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute('SELECT url FROM crawl_results') as cursor:
                    return [row[0] async for row in cursor]
        except Exception as e:
            logger.error(f"Error getting cached URLs: {str(e)}")
            return []
    
    async def save_results(self, results: List[CrawlResult]):
        """Save crawl results to database"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                for result in results:
                    if not isinstance(result, CrawlResult):
                        continue
                        
                    await db.execute('''
                        INSERT OR REPLACE INTO crawl_results (
                            url, success, html, cleaned_html,
                            error_message, media_data, links_data, metadata
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        result.url,
                        result.success,
                        result.html,
                        getattr(result, 'cleaned_html', None),
                        result.error_message,
                        json.dumps(result.media or {}),
                        json.dumps(result.links or {}),
                        json.dumps(getattr(result, 'metadata', {}) or {})
                    ))
                    self.url_cache.add(result.url)
                
                await db.commit()
                
        except Exception as e:
            logger.error(f"Error saving results: {str(e)}")
    
    async def get_result(self, url: str) -> Optional[CrawlResult]:
        """Get crawl result for specific URL"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                async with db.execute(
                    'SELECT * FROM crawl_results WHERE url = ?',
                    (url,)
                ) as cursor:
                    row = await cursor.fetchone()
                    if not row:
                        return None
                    
                    # Convert row to CrawlResult
                    return CrawlResult(
                        url=row[0],
                        success=bool(row[1]),
                        html=row[2],
                        cleaned_html=row[3],
                        error_message=row[4],
                        media=json.loads(row[5]) if row[5] else {},
                        links=json.loads(row[6]) if row[6] else {},
                        metadata=json.loads(row[7]) if row[7] else {}
                    )
                    
        except Exception as e:
            logger.error(f"Error getting result for {url}: {str(e)}")
            return None
    
    async def get_stats(self) -> Dict:
        """Get database statistics"""
        try:
            async with aiosqlite.connect(self.db_path) as db:
                # Get total counts
                async with db.execute('''
                    SELECT 
                        COUNT(*) as total,
                        SUM(CASE WHEN success = 1 THEN 1 ELSE 0 END) as successful,
                        SUM(CASE WHEN success = 0 THEN 1 ELSE 0 END) as failed
                    FROM crawl_results
                ''') as cursor:
                    row = await cursor.fetchone()
                    
                return {
                    'total_urls': row[0] or 0,
                    'successful': row[1] or 0,
                    'failed': row[2] or 0,
                    'cached_urls': len(self.url_cache),
                    'last_update': datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Error getting database stats: {str(e)}")
            return {}