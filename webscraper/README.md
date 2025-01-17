# Web Scraper with Crawl4AI Integration

A robust web scraper powered by Crawl4AI with comprehensive domain crawling, memory management, and anti-bot measures.

## Features

### Core Capabilities
- Full domain crawling with sitemap support
- Comprehensive link discovery and crawling
- Pattern-based URL exclusions
- Content storage in Markdown format
- Image and PDF downloading with metadata
- Screenshot capture
- Memory usage monitoring
- Built-in anti-bot protection
- Real-time progress tracking
- Browser automation with Crawl4AI

### Enhanced Features
- Parallel batch processing
- Memory-aware throttling
- Performance metrics tracking
- Content analytics
- URL pattern analysis
- Resource optimization
- Snowflake integration

## Project Structure

```
webscraper/
├── scraper.py           # Main scraping logic with Crawl4AI
├── app.py              # Streamlit dashboard
├── utils/
│   ├── monitors.py     # Memory and performance monitoring
│   ├── database.py     # Database operations
│   ├── processor.py    # Content processing
│   ├── history_analyzer.py  # Crawl history analysis
│   └── url_analyzer.py      # URL pattern analysis
├── SNOWFLAKE_INTEGRATION.md  # Snowflake integration details
└── requirements.txt    # Project dependencies
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

1. Enter the website URL to crawl
2. Configure crawling options:
   - Browser type (chromium/firefox/webkit)
   - Exclusion patterns
   - Output directory
   - Performance settings
3. Enable/disable advanced features:
   - Screenshot capture
   - PDF generation
   - Anti-bot protection
   - User simulation
4. Start crawling!

## Configuration

### Basic Settings
- **Browser Type**: Choose between chromium, firefox, or webkit
- **Test Mode**: Limits crawling to first 15 pages
- **Exclusion Patterns**: Regex patterns for URLs to exclude
- **Output Directory**: Where to store crawled content

### Performance Settings
- **Max Concurrent**: Maximum concurrent crawling sessions
- **Memory Threshold**: Memory usage limit before throttling
- **Request Rate**: Requests per second limit
- **Batch Size**: URLs to process in each batch

### Advanced Options
- **Screenshot Capture**: Enable automated screenshot capture
- **PDF Generation**: Generate PDF versions of pages
- **Anti-Bot Protection**: Enable advanced anti-bot measures
- **User Simulation**: Simulate human-like browsing behavior
- **Cache Control**: Configure caching behavior
- **Timeout Settings**: Customize page load timeouts

## Output Structure

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

## Advanced Usage

### URL Pattern Analysis
```python
from utils.url_analyzer import URLPatternAnalyzer

analyzer = URLPatternAnalyzer(base_url)
patterns = await analyzer.analyze_sitemap()
```

### Performance Monitoring
```python
from utils.monitors import MemoryMonitor, AntiBot

memory_monitor = MemoryMonitor(threshold_mb=1000)
anti_bot = AntiBot(requests_per_second=2.0)
```

### Content Processing
```python
from utils.processor import ContentProcessor

processor = ContentProcessor(domain_dir, domain)
result = await processor.process_crawl_result(crawl_result)
```

### History Analysis
```python
from utils.history_analyzer import CrawlHistoryAnalyzer

analyzer = CrawlHistoryAnalyzer(base_dir)
stats = analyzer.get_domain_stats(domain)
```

## Best Practices

### Memory Management
- Monitor memory usage through dashboard
- Set appropriate memory thresholds
- Use batch processing for large sites
- Enable caching for repeated crawls

### Anti-Bot Protection
- Enable magic mode for stealth
- Use appropriate request delays
- Rotate user agents
- Simulate user behavior

### Performance Optimization
- Use appropriate concurrency levels
- Enable browser reuse
- Configure caching strategy
- Monitor resource usage

### Error Handling
- Implement retry mechanisms
- Log failed requests
- Monitor error patterns
- Set appropriate timeouts

## Snowflake Integration

See [SNOWFLAKE_INTEGRATION.md](SNOWFLAKE_INTEGRATION.md) for detailed instructions on:
- Data storage structure
- Migration process
- Query patterns
- Analytics capabilities

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

### Development Setup
```bash
# Clone repository
git clone https://github.com/yourusername/webscraper.git

# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Install dependencies
pip install -r requirements.txt
```

### Running Tests
```bash
# Run unit tests
pytest tests/

# Run integration tests
pytest tests/integration/

# Check code style
flake8 webscraper/
```

## Troubleshooting

### Common Issues

1. Memory Usage
   - Check memory monitor output
   - Adjust batch size
   - Enable garbage collection

2. Anti-Bot Detection
   - Enable magic mode
   - Increase request delays
   - Use user simulation

3. Performance Issues
   - Check concurrent requests
   - Monitor CPU usage
   - Verify network conditions

4. Data Quality
   - Check content processor logs
   - Verify markdown output
   - Monitor resource downloads

### Getting Help

- Check the issues section
- Review error logs
- Contact maintainers

## License

MIT License - see [LICENSE](LICENSE) for details

## Acknowledgments

- Crawl4AI team for the excellent crawler framework
- Snowflake for database integration capabilities
- Streamlit for the dashboard framework
- Community contributors and users

