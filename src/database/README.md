# Database Module Documentation

This module handles all database operations for storing and retrieving crawler results using SQLite with async support.

## Overview

The database module provides persistent storage and caching functionality for web crawler results using SQLite as the backend database.

## Files Structure

```
database/
├── __init__.py     - Exports DatabaseManager
└── db_manager.py   - Contains DatabaseManager implementation
```

## Components Documentation

### DatabaseManager (db_manager.py)

Main class for handling all database operations with async support.

#### Class Signature
```python
class DatabaseManager:
    def __init__(self, db_path: str = None)
```

#### Methods

##### `initialize`
```python
async def initialize(self)
```
- Initializes database tables and caches
- Creates tables if they don't exist
- Thread-safe initialization with locking

##### `save_results`
```python
async def save_results(self, results: List[CrawlResult])
```
- Saves multiple crawl results to database
- Handles JSON serialization of complex data
- Updates URL cache

##### `get_result`
```python
async def get_result(self, url: str) -> Optional[CrawlResult]
```
- Retrieves crawl result for specific URL
- Deserializes data into CrawlResult object

##### `get_stats`
```python
async def get_stats(self) -> Dict
```
- Returns database statistics
- Includes success/failure counts and cache info

##### `get_cached_urls`
```python
async def get_cached_urls(self) -> List[str]
```
- Returns list of all cached URLs
- Used for cache initialization and verification

## Database Schema

### Table: crawl_results
```sql
CREATE TABLE crawl_results (
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
```

### Table: crawl_stats
```sql
CREATE TABLE crawl_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    start_time DATETIME,
    end_time DATETIME,
    total_urls INTEGER DEFAULT 0,
    successful_urls INTEGER DEFAULT 0,
    failed_urls INTEGER DEFAULT 0,
    metadata TEXT
)
```

## Key Features

1. **Async Database Operations**
   - Uses aiosqlite for async I/O
   - Thread-safe operations
   - Connection pooling

2. **Data Management**
   - URL caching
   - JSON serialization/deserialization
   - Error handling and logging

3. **Statistics Tracking**
   - Success/failure counts
   - Cache statistics
   - Timestamp tracking

4. **Performance Optimization**
   - In-memory URL cache
   - Batch operations support
   - Efficient query patterns

## Usage Example

```python
from database import DatabaseManager

# Initialize database manager
db = DatabaseManager()
await db.initialize()

# Save crawl results
await db.save_results(crawl_results)

# Get specific result
result = await db.get_result("https://example.com")

# Get database statistics
stats = await db.get_stats()
```

## Dependencies

- aiosqlite - Async SQLite operations
- asyncio - Async I/O support
- json - JSON data handling
- logging - Error and debug logging
- pathlib - File path management

## Implementation Details

1. **Thread Safety**
   - Uses asyncio.Lock for thread-safe initialization
   - Handles concurrent database access

2. **Error Handling**
   - Comprehensive error logging
   - Graceful failure recovery
   - Data validation

3. **Data Storage**
   - Efficient SQLite schema
   - JSON storage for complex data
   - Automatic timestamp tracking

4. **Cache Management**
   - In-memory URL cache
   - Automatic cache updates
   - Cache verification
