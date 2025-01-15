# Snowflake Integration Plan for Web Scraper Data

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
  │   ├── metadata/      # JSON metadata files
  │   └── stats.db       # SQLite database
```

### Current SQLite Schema
```sql
-- Pages table
CREATE TABLE pages (
    id INTEGER PRIMARY KEY,
    url TEXT UNIQUE,
    title TEXT,
    filepath TEXT,
    status TEXT,
    error_message TEXT,
    crawled_at TIMESTAMP
);

-- Images table
CREATE TABLE images (
    id INTEGER PRIMARY KEY,
    page_id INTEGER,
    url TEXT,
    filepath TEXT,
    FOREIGN KEY (page_id) REFERENCES pages(id)
);

-- Links table
CREATE TABLE links (
    id INTEGER PRIMARY KEY,
    page_id INTEGER,
    url TEXT,
    is_internal BOOLEAN,
    FOREIGN KEY (page_id) REFERENCES pages(id)
);
```

## Snowflake Integration Plan

### 1. Storage Structure

#### Internal Stages
```sql
-- Create file format for markdown
CREATE OR REPLACE FILE FORMAT markdown_format
  TYPE = 'TEXT'
  COMPRESSION = 'AUTO'
  FIELD_DELIMITER = 'NONE';

-- Create stages for different content types
CREATE OR REPLACE STAGE website_content.raw_markdown
  FILE_FORMAT = markdown_format
  DIRECTORY = (
    ENABLE = TRUE
  );

CREATE OR REPLACE STAGE website_content.images;
CREATE OR REPLACE STAGE website_content.pdfs;
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
    PRIMARY KEY (domain_id)
);

-- Page content and metadata
CREATE OR REPLACE TABLE website_pages (
    page_id VARCHAR NOT NULL,
    domain_id VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    title VARCHAR,
    markdown_path VARCHAR,  -- Path in stage
    status VARCHAR,
    crawled_at TIMESTAMP_NTZ,
    error_message VARCHAR,
    metadata VARIANT,  -- Additional metadata as JSON
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    updated_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (page_id),
    FOREIGN KEY (domain_id) REFERENCES website_domains(domain_id)
);

-- Page resources (images, PDFs)
CREATE OR REPLACE TABLE page_resources (
    resource_id VARCHAR NOT NULL,
    page_id VARCHAR NOT NULL,
    resource_type VARCHAR NOT NULL,  -- 'IMAGE' or 'PDF'
    original_url VARCHAR,
    stage_path VARCHAR,  -- Path in respective stage
    file_size BIGINT,
    mime_type VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (resource_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);

-- Page links
CREATE OR REPLACE TABLE page_links (
    link_id VARCHAR NOT NULL,
    page_id VARCHAR NOT NULL,
    target_url VARCHAR NOT NULL,
    is_internal BOOLEAN,
    link_text VARCHAR,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (link_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);

-- Crawl history
CREATE OR REPLACE TABLE crawl_history (
    crawl_id VARCHAR NOT NULL,
    domain_id VARCHAR NOT NULL,
    start_time TIMESTAMP_NTZ,
    end_time TIMESTAMP_NTZ,
    total_pages INTEGER,
    successful_pages INTEGER,
    failed_pages INTEGER,
    error_details VARIANT,  -- JSON for detailed errors
    PRIMARY KEY (crawl_id),
    FOREIGN KEY (domain_id) REFERENCES website_domains(domain_id)
);
```

### 2. Data Migration Process

#### Stage Upload Process
1. **Organize Files**
   ```python
   def organize_files_for_upload(domain_dir: Path):
       """
       1. Flatten directory structure
       2. Create consistent naming pattern
       3. Generate manifest files
       """
       # Implementation details...
   ```

2. **Upload to Stages**
   ```python
   def upload_to_snowflake_stages(files: Dict[str, Path]):
       """
       1. Use PUT command for uploads
       2. Verify uploads
       3. Track stage paths
       """
       # Implementation details...
   ```

3. **Metadata Processing**
   ```python
   def process_metadata_for_snowflake(sqlite_db: Path):
       """
       1. Extract from SQLite
       2. Transform for Snowflake schema
       3. Generate INSERT statements
       """
       # Implementation details...
   ```

### 3. Future-Proofing Considerations

1. **Versioning**
   - Add version tracking for content
   - Support multiple versions of same page
   - Track content changes over time

```sql
-- Version tracking table
CREATE OR REPLACE TABLE page_versions (
    version_id VARCHAR NOT NULL,
    page_id VARCHAR NOT NULL,
    markdown_path VARCHAR,
    version_number INTEGER,
    changes_summary VARIANT,
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (version_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);
```

2. **Content Analysis**
   - Full-text search capabilities
   - Content classification
   - Topic extraction

```sql
-- Content analysis table
CREATE OR REPLACE TABLE page_analytics (
    page_id VARCHAR NOT NULL,
    topics ARRAY,
    keywords ARRAY,
    readability_score FLOAT,
    content_category VARCHAR,
    analyzed_at TIMESTAMP_NTZ,
    PRIMARY KEY (page_id),
    FOREIGN KEY (page_id) REFERENCES website_pages(page_id)
);
```

3. **Resource Management**
   - Deduplication of resources
   - Resource versioning
   - Resource usage tracking

```sql
-- Resource deduplication
CREATE OR REPLACE TABLE resource_fingerprints (
    fingerprint_id VARCHAR NOT NULL,
    content_hash VARCHAR,
    first_seen_at TIMESTAMP_NTZ,
    resource_count INTEGER,
    PRIMARY KEY (fingerprint_id)
);
```

### 4. Implementation Steps

1. **Stage Setup**
   ```bash
   # Create directories for staging
   mkdir -p snowflake_staging/{markdown,images,pdfs}
   ```

2. **Data Migration Script**
   ```python
   # migration.py
   from pathlib import Path
   import snowflake.connector
   from typing import Dict, List

   class SnowflakeUploader:
       def __init__(self, config: Dict):
           self.conn = snowflake.connector.connect(**config)
           
       def upload_domain(self, domain_dir: Path):
           # Implementation...
   ```

3. **Verification Process**
   ```python
   class DataVerifier:
       def verify_uploads(self):
           # Check file counts
           # Verify content integrity
           # Validate relationships
   ```

4. **Maintenance Procedures**
   ```sql
   -- Regular maintenance
   CREATE OR REPLACE PROCEDURE cleanup_unused_resources()
   ...

   CREATE OR REPLACE PROCEDURE optimize_storage()
   ...
   ```

### 5. Query Examples

```sql
-- Get all pages for a domain with resources
SELECT 
    p.page_id,
    p.title,
    p.markdown_path,
    ARRAY_AGG(DISTINCT pr.stage_path) as resources,
    ARRAY_AGG(DISTINCT pl.target_url) as links
FROM website_pages p
LEFT JOIN page_resources pr ON p.page_id = pr.page_id
LEFT JOIN page_links pl ON p.page_id = pl.page_id
WHERE p.domain_id = ?
GROUP BY p.page_id, p.title, p.markdown_path;

-- Get content analysis for domain
SELECT 
    d.domain_name,
    COUNT(DISTINCT p.page_id) as total_pages,
    AVG(pa.readability_score) as avg_readability,
    ARRAY_AGG(DISTINCT pa.content_category) as categories
FROM website_domains d
JOIN website_pages p ON d.domain_id = p.domain_id
JOIN page_analytics pa ON p.page_id = pa.page_id
GROUP BY d.domain_name;
```

## Next Steps

1. Create migration scripts
2. Set up staging environment
3. Implement verification procedures
4. Create maintenance schedules
5. Document query patterns
6. Set up monitoring and alerts

Would you like me to elaborate on any particular aspect or provide more examples for a specific component?