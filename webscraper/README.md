# Advanced Web Scraper with Memory Management and Anti-Bot Features

A robust web scraper with comprehensive memory management, anti-bot measures, and a Streamlit dashboard for monitoring and analysis.

## Features

### Core Capabilities
- Full domain crawling with sitemap support
- Markdown content generation
- Image and PDF downloading
- Complete link coverage tracking
- Structured metadata storage
- Memory-aware processing
- Anti-bot protection
- Real-time progress monitoring
- Comprehensive reporting

### Memory Management
- Batch processing with configurable sizes
- Memory usage monitoring with psutil
- Automatic pausing on high memory
- Cache clearing between batches
- Resource cleanup

### Anti-Bot Measures
- User agent rotation
- Random delays between requests
- Stealth mode with magic=True
- Request rate limiting
- Human behavior simulation

### Data Processing
- Sitemap-based URL discovery
- Recursive sitemap index handling
- Image and PDF downloading
- Internal/external link tracking
- Metadata extraction
- Progress persistence

## Project Structure

```
webscraper/
├── scraper.py           # Main scraping logic
├── app.py              # Streamlit dashboard
├── utils/
│   ├── monitors.py     # Memory and anti-bot monitoring
│   ├── database.py     # Database operations
│   └── processor.py    # Content processing
└── requirements.txt    # Project dependencies
```

## Implementation Details

### 1. Core Scraper (scraper.py)
The main scraper implementation handling the crawling process.

```python
class WebScraper:
    def __init__(
        self,
        base_url: str,
        output_dir: str,
        max_concurrent: int = 5,
        requests_per_second: float = 2.0,
        memory_threshold_mb: int = 1000,
        batch_size: int = 10
    ):
        # Initialize core components
        self.memory_monitor = MemoryMonitor(memory_threshold_mb)
        self.anti_bot = AntiBot(requests_per_second)
        self.db = DatabaseHandler(self.domain_dir / 'stats.db')
        self.processor = ContentProcessor(self.domain_dir, self.domain)
```

Key methods:
- `discover_sitemap_urls()`: Extracts URLs from sitemap.xml
- `process_url()`: Handles single URL processing
- `process_batch()`: Manages batch processing with memory checks
- `crawl()`: Main crawling loop with anti-bot measures

### 2. Memory Monitoring (utils/monitors.py)
Handles memory management and anti-bot measures.

```python
class MemoryMonitor:
    def __init__(self, threshold_mb: int = 1000):
        self.process = psutil.Process()
        self.threshold_mb = threshold_mb
    
    def check_memory(self) -> bool:
        memory_mb = self.process.memory_info().rss / 1024 / 1024
        return memory_mb < self.threshold_mb

class AntiBot:
    async def random_delay(self):
        delay = random.uniform(1.0, 3.0) / self.requests_per_second
        await asyncio.sleep(delay)
```

Features:
- Memory usage tracking
- Configurable thresholds
- User agent rotation
- Random delay generation

### 3. Database Handler (utils/database.py)
Manages data persistence and statistics.

```python
class DatabaseHandler:
    def _setup_tables(self):
        """Initialize database schema"""
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
            # ... other tables
        ''')
```

Tables:
- pages: Stores page metadata
- images: Tracks downloaded images
- links: Records page links
- crawl_stats: Maintains crawling statistics

### 4. Content Processor (utils/processor.py)
Handles content processing and file management.

```python
class ContentProcessor:
    async def process_images(self, html: str, base_url: str) -> List[Dict]:
        """Process and download images from HTML"""
        soup = BeautifulSoup(html, 'html.parser')
        images = []
        for img in soup.find_all('img', src=True):
            img_url = urljoin(base_url, img['src'])
            # Download and track images
```

Features:
- Image downloading
- PDF processing
- Link extraction
- Markdown generation
- Title extraction

### 5. Streamlit Dashboard (app.py)
Interactive monitoring and reporting interface.

```python
def display_crawl_status(stats: dict):
    """Display current crawling status"""
    st.header("Crawling Status")
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total URLs", stats['total_urls'])
    col2.metric("Successful", stats['successful'])
    col3.metric("Failed", stats['failed'])
    col4.metric("Success Rate", f"{stats['success_rate']:.1f}%")
```

Features:
- Real-time progress monitoring
- Interactive visualizations
- Detailed page information
- Error reporting
- Downloadable reports

## Output Structure

```
output_dir/
  ├── domain.com/
  │   ├── content/        # Markdown files
  │   ├── images/         # Downloaded images
  │   ├── pdfs/          # Downloaded PDFs
  │   ├── metadata/      # JSON metadata files
  │   └── stats.db       # SQLite database
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the Streamlit dashboard:
```bash
streamlit run app.py
```

## Usage

1. Start the dashboard:
```bash
streamlit run app.py
```

2. Enter the website URL and output directory
3. Monitor progress in real-time
4. View statistics and download reports
5. Check the stats.db for detailed information

## Best Practices

1. Memory Management:
- Keep batch_size reasonable (default: 10)
- Set appropriate memory_threshold_mb
- Monitor memory usage logs
- Clear caches regularly

2. Anti-Bot Protection:
- Use reasonable requests_per_second
- Enable magic mode
- Utilize random delays
- Rotate user agents

3. Error Handling:
- Check logs for errors
- Monitor failed URLs
- Review error messages
- Adjust timeouts if needed

4. Data Management:
- Regularly backup stats.db
- Clean up old content
- Monitor disk usage
- Review crawl statistics

## Error Handling

The scraper implements comprehensive error handling:
- URL-level error tracking
- Batch processing failure recovery
- Memory overflow protection
- Database transaction safety
- Resource cleanup

## Performance Optimization

1. Batch Processing:
- Configurable batch sizes
- Memory-aware processing
- Automatic pausing
- Cache management

2. Resource Management:
- Connection pooling
- File handle cleanup
- Memory monitoring
- Garbage collection

3. Concurrent Processing:
- Async/await patterns
- Semaphore control
- Rate limiting
- Resource sharing

## Monitoring and Logging

1. Real-time Monitoring:
- Memory usage tracking
- URL processing status
- Error reporting
- Progress indicators

2. Logging:
- Detailed error logs
- Processing statistics
- Performance metrics
- Status updates

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License - feel free to use in your own projects.