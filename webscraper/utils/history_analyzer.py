import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class CrawlHistoryAnalyzer:
    """Analyzes crawl history across all domains"""
    
    def __init__(self, base_dir: str = '/tmp/webscraper'):
        self.base_dir = Path(base_dir)
    
    def get_domains(self) -> List[str]:
        """Get list of all crawled domains"""
        return [d.name for d in self.base_dir.iterdir() if d.is_dir()]
    
    def get_domain_stats(self, domain: str) -> Dict:
        """Get statistics for a domain"""
        try:
            db_path = self.base_dir / domain / 'stats.db'
            if not db_path.exists():
                return {}
            
            conn = sqlite3.connect(str(db_path))
            
            # Get crawl history
            crawls_df = pd.read_sql_query('''
                SELECT 
                    start_time,
                    end_time,
                    total_urls,
                    successful,
                    failed,
                    current_memory_usage
                FROM crawl_stats
                ORDER BY start_time DESC
            ''', conn)
            
            # Get page statistics
            pages_df = pd.read_sql_query('''
                SELECT 
                    COUNT(*) as total_pages,
                    COUNT(CASE WHEN status = 'success' THEN 1 END) as successful_pages,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_pages,
                    MAX(crawled_at) as last_crawl
                FROM pages
            ''', conn)
            
            # Get resource statistics
            images_df = pd.read_sql_query('''
                SELECT COUNT(*) as total_images
                FROM images
            ''', conn)
            
            conn.close()
            
            return {
                'domain': domain,
                'crawls': len(crawls_df),
                'last_crawl': crawls_df['end_time'].iloc[0] if not crawls_df.empty else None,
                'total_pages': pages_df['total_pages'].iloc[0],
                'successful_pages': pages_df['successful_pages'].iloc[0],
                'failed_pages': pages_df['failed_pages'].iloc[0],
                'total_images': images_df['total_images'].iloc[0],
                'crawl_history': crawls_df.to_dict('records'),
                'success_rate': (pages_df['successful_pages'].iloc[0] / pages_df['total_pages'].iloc[0] * 100) 
                               if pages_df['total_pages'].iloc[0] > 0 else 0
            }
        except Exception as e:
            logger.error(f"Error getting stats for {domain}: {str(e)}")
            return {}
    
    def get_all_stats(self) -> List[Dict]:
        """Get statistics for all domains"""
        stats = []
        for domain in self.get_domains():
            domain_stats = self.get_domain_stats(domain)
            if domain_stats:
                stats.append(domain_stats)
        return stats
