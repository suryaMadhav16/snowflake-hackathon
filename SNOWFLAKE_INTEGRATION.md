# Snowflake Integration Plan for Crawl4AI Data

## Current Data Structure

### Directory Structure
```
output_dir/
  ├── domain.com/
  │   ├── content/         # Markdown files
  │   │   └── {hash}.md   # Content files
  │   ├── images/         # Downloaded images
  │   │   └── {hash}.{ext}
  │   ├── pdfs/          # Downloaded PDFs
  │   │   └── {hash}.pdf
  │   ├── screenshots/    # Page screenshots
  │   │   └── {hash}.png
  │   └── stats.db       # SQLite database
```

### Current SQLite Schema with Crawl4AI Extensions
```sql
-- Pages table
CREATE TABLE pages (
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

-- Images table with enhanced metadata
CREATE TABLE images (
    id INTEGER PRIMARY KEY,
    page_id INTEGER,
    url TEXT,
    filepath TEXT,
    alt_text TEXT,
    score REAL,
    size INTEGER,
    FOREIGN KEY (page_id) REFERENCES pages(id)
);

-- Links table with enhanced metadata
CREATE TABLE links (
    id INTEGER PRIMARY KEY,
    page_id INTEGER,
    url TEXT,
    is_internal BOOLEAN,
    text_content TEXT,
    metadata JSON,
    FOREIGN KEY (page_id) REFERENCES pages(id)
);

-- Crawl stats with performance metrics
CREATE TABLE crawl_stats (
    id INTEGER PRIMARY KEY,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    total_urls INTEGER,
    successful INTEGER,
    failed INTEGER,
    current_memory_usage REAL,
    config JSON,
    performance_metrics JSON
);

-- Performance metrics tracking
CREATE TABLE performance_metrics (
    id INTEGER PRIMARY KEY,
    crawl_id INTEGER,
    timestamp TIMESTAMP,
    memory_usage REAL,
    cpu_usage REAL,
    active_sessions INTEGER,
    errors_count INTEGER,
    FOREIGN KEY (crawl_id) REFERENCES crawl_stats(id)
);
```

## Snowflake Integration Plan

### 1. Storage Structure

#### Internal Stages
```sql
-- Create file formats
CREATE OR REPLACE FILE FORMAT markdown_format
  TYPE = 'TEXT'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = 'NONE';

CREATE OR REPLACE FILE FORMAT json_format
  TYPE = 'JSON'
  COMPRESSION = 'AUTO'
  STRIP_OUTER_ARRAY = TRUE;

-- Create stages for different content types
CREATE OR REPLACE STAGE website_content.raw_content
  FILE_FORMAT = markdown_format
  DIRECTORY = (
    ENABLE = TRUE
  );

CREATE OR REPLACE STAGE website_content.media;
CREATE OR REPLACE STAGE website_content.metrics;
```

#### Table Structure
```sql
-- Domain tracking
CREATE OR REPLACE TABLE website_domains (
    domain_id VARCHAR NOT NULL,
    domain_name VARCHAR NOT NULL,
    first_crawled_at TIMESTAMP_NTZ,
    last_crawled_at TIMESTAMP_NTZ,
    total_pages INTEGER,
    crawler_config VARIANT,  -- Crawl4AI configuration
    PRIMARY KEY (domain_id)
);

-- Page content and metadata
CREATE OR REPLACE TABLE website_pages (
    page_id VARCHAR NOT NULL,
    domain_id VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    title VARCHAR,
    content_path VARCHAR,  -- Path in stage
    status VARCHAR,
    word_count INTEGER,
    crawled_at TIMESTAMP_NTZ,
    error_message VARCHAR,
    screenshot_path VARCHAR,
    pdf_path VARCHAR,
    metadata VARIANT,  -- Crawl4AI metadata
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (page_id),
    FOREIGN KEY (domain_id) REFERENCES website_domains(domain_id)
);

-- Media resources (images, PDFs, screenshots)
CREATE OR REPLACE TABLE page_media (
    media_id VARCHAR NOT NULL,
    page_id VARCHAR NOT NULL,
    media_type VARCHAR NOT NULL,  -- 'IMAGE', 'PDF', 'SCREENSHOT'
    original_url VARCHAR,
    stage_path VARCHAR,
    file_size BIGINT,
    alt_text VARCHAR,
    score FLOAT,  -- Crawl4AI image score
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (media_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);

-- Page links with enhanced metadata
CREATE OR REPLACE TABLE page_links (
    link_id VARCHAR NOT NULL,
    page_id VARCHAR NOT NULL,
    target_url VARCHAR NOT NULL,
    is_internal BOOLEAN,
    text_content VARCHAR,
    metadata VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (link_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);

-- Crawl history with performance metrics
CREATE OR REPLACE TABLE crawl_history (
    crawl_id VARCHAR NOT NULL,
    domain_id VARCHAR NOT NULL,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    total_urls INTEGER,
    successful_urls INTEGER,
    failed_urls INTEGER,
    config VARIANT,  -- Crawl4AI configuration
    performance_metrics VARIANT,  -- Aggregated metrics
    PRIMARY KEY (crawl_id),
    FOREIGN KEY (domain_id) REFERENCES website_domains(domain_id)
);

-- Detailed performance metrics
CREATE OR REPLACE TABLE performance_metrics (
    metric_id VARCHAR NOT NULL,
    crawl_id VARCHAR NOT NULL,
    timestamp TIMESTAMP_NTZ,
    metric_type VARCHAR,  -- 'MEMORY', 'CPU', 'LOAD_TIME'
    metric_value FLOAT,
    metadata VARIANT,
    PRIMARY KEY (metric_id),
    FOREIGN KEY (crawl_id) REFERENCES crawl_history(crawl_id)
);
```

### 2. Data Migration Process

#### Stage Upload Process
```python
from pathlib import Path
import snowflake.connector
from typing import Dict, List
import json

class Crawl4AISnowflakeUploader:
    def __init__(self, config: Dict):
        self.conn = snowflake.connector.connect(**config)
        
    async def upload_crawl_data(self, domain_dir: Path, crawl_result):
        """Upload Crawl4AI results to Snowflake"""
        try:
            # Upload content files
            content_paths = await self._upload_content_files(domain_dir)
            
            # Upload media files
            media_paths = await self._upload_media_files(domain_dir)
            
            # Upload metadata
            await self._upload_metadata(crawl_result)
            
            # Upload performance metrics
            await self._upload_metrics(crawl_result.metrics)
            
        except Exception as e:
            logger.error(f"Error uploading to Snowflake: {str(e)}")
```

### 3. Analysis Features

#### Content Analysis Views
```sql
-- Page content analysis
CREATE OR REPLACE VIEW content_analytics AS
SELECT 
    d.domain_name,
    p.url,
    p.title,
    p.word_count,
    m.media_count,
    l.link_count,
    pm.avg_load_time,
    pm.avg_memory_usage
FROM website_pages p
JOIN website_domains d ON p.domain_id = d.domain_id
LEFT JOIN (
    SELECT page_id, COUNT(*) as media_count 
    FROM page_media 
    GROUP BY page_id
) m ON p.page_id = m.page_id
LEFT JOIN (
    SELECT page_id, COUNT(*) as link_count 
    FROM page_links 
    GROUP BY page_id
) l ON p.page_id = l.page_id
LEFT JOIN (
    SELECT 
        page_id,
        AVG(CASE WHEN metric_type = 'LOAD_TIME' THEN metric_value END) as avg_load_time,
        AVG(CASE WHEN metric_type = 'MEMORY' THEN metric_value END) as avg_memory_usage
    FROM performance_metrics
    GROUP BY page_id
) pm ON p.page_id = pm.page_id;
```

#### Performance Analysis
```sql
-- Crawl performance analytics
CREATE OR REPLACE VIEW crawl_performance AS
SELECT 
    ch.crawl_id,
    ch.start_time,
    ch.end_time,
    ch.total_urls,
    ch.successful_urls,
    ch.failed_urls,
    pm.avg_memory_usage,
    pm.avg_cpu_usage,
    pm.avg_load_time,
    config:browser_type::STRING as browser_type,
    config:magic::BOOLEAN as anti_bot_enabled
FROM crawl_history ch
LEFT JOIN (
    SELECT 
        crawl_id,
        AVG(CASE WHEN metric_type = 'MEMORY' THEN metric_value END) as avg_memory_usage,
        AVG(CASE WHEN metric_type = 'CPU' THEN metric_value END) as avg_cpu_usage,
        AVG(CASE WHEN metric_type = 'LOAD_TIME' THEN metric_value END) as avg_load_time
    FROM performance_metrics
    GROUP BY crawl_id
) pm ON ch.crawl_id = pm.crawl_id;
```

### 4. Implementation Steps

1. **Initial Setup**
   ```bash
   # Set up Python environment
   pip install snowflake-connector-python
   pip install crawl4ai
   ```

2. **Data Migration Script**
   ```python
   # snowflake_uploader.py
   from snowflake_integration import Crawl4AISnowflakeUploader
   
   async def migrate_to_snowflake(config: Dict):
       uploader = Crawl4AISnowflakeUploader(config)
       await uploader.start_migration()
   ```

3. **Verify Data Quality**
   ```sql
   -- Verify data consistency
   SELECT 
       COUNT(*) as total_pages,
       COUNT(DISTINCT domain_id) as total_domains,
       SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_pages
   FROM website_pages;
   ```

4. **Monitor Performance**
   ```sql
   -- Monitor storage usage
   SELECT 
       TABLE_NAME,
       ROW_COUNT,
       BYTES/1024/1024 as size_mb
   FROM INFORMATION_SCHEMA.TABLES
   WHERE TABLE_SCHEMA = 'WEBSITE_CONTENT';
   ```

### 5. Future Enhancements

1. **Machine Learning Integration**
   ```sql
   -- Create ML-ready view
   CREATE OR REPLACE VIEW ml_content_features AS
   SELECT 
       p.page_id,
       p.word_count,
       p.metadata:text_density::FLOAT as text_density,
       p.metadata:content_quality_score::FLOAT as quality_score,
       m.media_count,
       l.link_count
   FROM website_pages p
   LEFT JOIN /* ... */;
   ```

2. **Real-time Analytics**
   ```sql
   -- Create real-time dashboard view
   CREATE OR REPLACE VIEW realtime_crawl_metrics AS
   SELECT 
       date_trunc('hour', timestamp) as time_bucket,
       COUNT(DISTINCT crawl_id) as active_crawls,
       AVG(metric_value) as avg_metric_value,
       metric_type
   FROM performance_metrics
   WHERE timestamp >= dateadd('hour', -24, current_timestamp())
   GROUP BY 1, metric_type
   ORDER BY 1 DESC;
   ```

3. **Advanced Pattern Analysis**
   ```sql
   -- URL pattern analysis
   CREATE OR REPLACE VIEW url_patterns AS
   SELECT 
       regexp_substr(url, '[^/]+', 1, level) as url_segment,
       COUNT(*) as segment_count,
       AVG(word_count) as avg_content_size
   FROM website_pages,
   TABLE(split_to_table(url, '/'))
   GROUP BY 1
   HAVING segment_count > 10
   ORDER BY segment_count DESC;
   ```

## Next Steps

1. Implement data migration pipeline
2. Set up automated testing
3. Create monitoring dashboards
4. Establish backup procedures
5. Document query patterns
6. Set up alerting system

For specific implementation details or additional examples, please refer to the documentation or raise an issue in the repository.