import sqlite3
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)

class CrawlHistoryAnalyzer:
    """Analyzes crawl history across all domains for Crawl4AI results"""
    
    def __init__(self, base_dir: str = '/tmp/webscraper'):
        self.base_dir = Path(base_dir)
    
    def get_domains(self) -> List[str]:
        """Get list of all crawled domains"""
        return [d.name for d in self.base_dir.iterdir() 
                if d.is_dir() and (d / 'stats.db').exists()]
    
    def get_domain_stats(self, domain: str) -> Dict:
        """Get comprehensive statistics for a domain
        
        Args:
            domain: Domain name to analyze
            
        Returns:
            Dictionary containing domain statistics and metrics
        """
        try:
            db_path = self.base_dir / domain / 'stats.db'
            if not db_path.exists():
                return {}
            
            conn = sqlite3.connect(str(db_path))
            
            # Get detailed crawl history
            crawls_df = pd.read_sql_query('''
                SELECT 
                    cs.*,
                    COUNT(DISTINCT p.id) as pages_count,
                    COUNT(DISTINCT i.id) as images_count,
                    COUNT(DISTINCT l.id) as links_count,
                    SUM(CASE WHEN p.pdf_path IS NOT NULL THEN 1 ELSE 0 END) as pdfs_count,
                    SUM(CASE WHEN p.screenshot_path IS NOT NULL THEN 1 ELSE 0 END) as screenshots_count,
                    AVG(p.word_count) as avg_word_count
                FROM crawl_stats cs
                LEFT JOIN pages p ON p.crawled_at BETWEEN cs.start_time AND COALESCE(cs.end_time, datetime('now'))
                LEFT JOIN images i ON i.page_id = p.id
                LEFT JOIN links l ON l.page_id = p.id
                GROUP BY cs.id
                ORDER BY cs.start_time DESC
            ''', conn)
            
            # Get performance metrics history
            perf_df = pd.read_sql_query('''
                SELECT 
                    crawl_id,
                    AVG(memory_usage) as avg_memory_usage,
                    MAX(memory_usage) as peak_memory_usage,
                    AVG(cpu_usage) as avg_cpu_usage,
                    MAX(cpu_usage) as peak_cpu_usage,
                    COUNT(*) as metric_points
                FROM performance_metrics
                GROUP BY crawl_id
            ''', conn)
            
            # Get content type breakdown
            content_df = pd.read_sql_query('''
                SELECT
                    COUNT(DISTINCT CASE WHEN filepath LIKE '%.md' THEN id END) as markdown_files,
                    COUNT(DISTINCT CASE WHEN screenshot_path IS NOT NULL THEN id END) as screenshots,
                    COUNT(DISTINCT CASE WHEN pdf_path IS NOT NULL THEN id END) as pdfs,
                    AVG(CASE WHEN word_count > 0 THEN word_count END) as avg_content_length
                FROM pages
            ''', conn)
            
            # Get error statistics
            errors_df = pd.read_sql_query('''
                SELECT 
                    error_message,
                    COUNT(*) as count
                FROM pages
                WHERE status = 'failed'
                GROUP BY error_message
                ORDER BY count DESC
                LIMIT 5
            ''', conn)
            
            # Process crawl configurations
            configs = []
            for _, row in crawls_df.iterrows():
                config = json.loads(row['config']) if row['config'] else {}
                perf_metrics = json.loads(row['performance_metrics']) if row['performance_metrics'] else {}
                configs.append({
                    'crawl_id': row.name,
                    'config': config,
                    'performance': perf_metrics
                })
            
            # Combine all statistics
            stats = {
                'domain': domain,
                'summary': {
                    'total_crawls': len(crawls_df),
                    'last_crawl': crawls_df['end_time'].iloc[0] if not crawls_df.empty else None,
                    'total_pages': crawls_df['pages_count'].sum(),
                    'total_images': crawls_df['images_count'].sum(),
                    'total_links': crawls_df['links_count'].sum(),
                    'total_pdfs': crawls_df['pdfs_count'].sum(),
                    'total_screenshots': crawls_df['screenshots_count'].sum(),
                    'avg_word_count': crawls_df['avg_word_count'].mean()
                },
                'performance': {
                    'avg_memory_usage': perf_df['avg_memory_usage'].mean(),
                    'peak_memory_usage': perf_df['peak_memory_usage'].max(),
                    'avg_cpu_usage': perf_df['avg_cpu_usage'].mean(),
                    'peak_cpu_usage': perf_df['peak_cpu_usage'].max()
                },
                'content_stats': content_df.iloc[0].to_dict(),
                'top_errors': errors_df.to_dict('records'),
                'crawl_history': crawls_df.to_dict('records'),
                'configurations': configs
            }
            
            conn.close()
            return stats
            
        except Exception as e:
            logger.error(f"Error getting stats for {domain}: {str(e)}")
            return {}
    
    def get_all_stats(self) -> List[Dict]:
        """Get statistics for all domains"""
        return [stats for domain in self.get_domains()
                if (stats := self.get_domain_stats(domain))]
    
    def get_performance_comparison(self) -> pd.DataFrame:
        """Compare performance metrics across domains"""
        stats = []
        for domain in self.get_domains():
            try:
                db_path = self.base_dir / domain / 'stats.db'
                if not db_path.exists():
                    continue
                
                conn = sqlite3.connect(str(db_path))
                df = pd.read_sql_query('''
                    SELECT 
                        'latest' as crawl_type,
                        cs.id as crawl_id,
                        cs.start_time,
                        cs.end_time,
                        cs.total_urls,
                        cs.successful,
                        cs.failed,
                        COUNT(DISTINCT p.id) as pages_processed,
                        AVG(pm.memory_usage) as avg_memory_mb,
                        AVG(pm.cpu_usage) as avg_cpu_percent,
                        COUNT(DISTINCT i.id) as images_processed,
                        COUNT(DISTINCT l.id) as links_processed
                    FROM crawl_stats cs
                    LEFT JOIN pages p ON p.crawled_at BETWEEN cs.start_time AND cs.end_time
                    LEFT JOIN performance_metrics pm ON pm.crawl_id = cs.id
                    LEFT JOIN images i ON i.page_id = p.id
                    LEFT JOIN links l ON l.page_id = p.id
                    WHERE cs.id = (SELECT MAX(id) FROM crawl_stats)
                    GROUP BY cs.id
                ''', conn)
                
                if not df.empty:
                    df['domain'] = domain
                    stats.append(df)
                
                conn.close()
                
            except Exception as e:
                logger.error(f"Error getting performance comparison for {domain}: {str(e)}")
                continue
        
        if not stats:
            return pd.DataFrame()
        
        return pd.concat(stats, ignore_index=True)
    
    def analyze_crawl_trends(self, domain: str) -> Dict:
        """Analyze crawling trends over time for a domain
        
        Args:
            domain: Domain to analyze
            
        Returns:
            Dictionary containing trend analysis
        """
        try:
            db_path = self.base_dir / domain / 'stats.db'
            if not db_path.exists():
                return {}
                
            conn = sqlite3.connect(str(db_path))
            
            # Get trends data
            trends_df = pd.read_sql_query('''
                SELECT 
                    date(start_time) as crawl_date,
                    COUNT(*) as crawls_count,
                    SUM(total_urls) as total_urls,
                    SUM(successful) as successful,
                    SUM(failed) as failed,
                    AVG(current_memory_usage) as avg_memory_usage
                FROM crawl_stats
                GROUP BY date(start_time)
                ORDER BY crawl_date
            ''', conn)
            
            # Calculate metrics
            if not trends_df.empty:
                trends_df['success_rate'] = (trends_df['successful'] / trends_df['total_urls'] * 100).round(2)
                
                return {
                    'daily_stats': trends_df.to_dict('records'),
                    'trends': {
                        'success_rate_trend': trends_df['success_rate'].tolist(),
                        'urls_trend': trends_df['total_urls'].tolist(),
                        'memory_trend': trends_df['avg_memory_usage'].tolist()
                    },
                    'summary': {
                        'avg_success_rate': trends_df['success_rate'].mean(),
                        'avg_daily_urls': trends_df['total_urls'].mean(),
                        'total_days': len(trends_df)
                    }
                }
                
            return {}
            
        except Exception as e:
            logger.error(f"Error analyzing trends for {domain}: {str(e)}")
            return {}