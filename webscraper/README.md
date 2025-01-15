# Advanced Web Scraper with URL Pattern Selection and History Tracking

A robust web scraper with comprehensive memory management, anti-bot measures, URL pattern selection, history tracking, and a Streamlit dashboard for monitoring and analysis.

## Features

### Core Capabilities
- Full domain crawling with sitemap support
- URL pattern analysis and selective crawling
- Markdown content generation
- Image and PDF downloading
- Complete link coverage tracking
- Structured metadata storage
- Memory-aware processing
- Anti-bot protection
- Real-time progress monitoring
- Comprehensive reporting
- Historical crawl tracking

### New Features
1. **URL Pattern Analysis**
   - Automatic sitemap discovery
   - Pattern extraction and grouping
   - URL count per pattern
   - Selective pattern crawling
   - Dynamic URL filtering

2. **Crawl History**
   - Domain-wise crawl history
   - Success rate tracking
   - Memory usage graphs
   - Detailed statistics per domain
   - Timeline visualization

3. **Enhanced UI**
   - Two-page navigation (Crawler & History)
   - Pattern selection interface
   - Real-time memory graphs
   - Batch progress tracking
   - Historical data visualization

### Previous Features
- Memory Management
- Anti-Bot Measures
- Data Processing
- Sitemap Support
- Error Handling

## Project Structure

```
webscraper/
├── scraper.py           # Main scraping logic
├── app.py              # Streamlit dashboard
├── utils/
│   ├── monitors.py     # Memory and anti-bot monitoring
│   ├── database.py     # Database operations
│   ├── processor.py    # Content processing
│   ├── url_analyzer.py # URL pattern analysis
│   └── history_analyzer.py # Crawl history analysis
└── requirements.txt    # Project dependencies
```

## Implementation Details

### 1. URL Pattern Analyzer (utils/url_analyzer.py)
```python
class URLPatternAnalyzer:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.domain = urlparse(base_url).netloc
        self.patterns = defaultdict(set)
        
    def _extract_pattern(self, url: str) -> str:
        # URL pattern extraction logic
        path = parsed.path
        path = re.sub(r'/\d+', '/{n}', path)
        path = re.sub(r'/[a-f0-9]{8,}', '/{id}', path)
        path = re.sub(r'/\d{4}/\d{2}/\d{2}', '/{date}', path)
        return path
```

### 2. History Analyzer (utils/history_analyzer.py)
```python
class CrawlHistoryAnalyzer:
    def get_domain_stats(self, domain: str) -> Dict:
        # Get statistics for each domain
        return {
            'domain': domain,
            'crawls': len(crawls_df),
            'total_pages': pages_df['total_pages'].iloc[0],
            'success_rate': success_rate,
            'crawl_history': crawls_df.to_dict('records')
        }
```

### 3. Enhanced Scraper (scraper.py)
```python
class WebScraper:
    def __init__(self, ..., test_mode: bool = False):
        self.test_mode = test_mode
        self.override_discovered_urls = None

    async def crawl(self):
        if self.override_discovered_urls:
            self.discovered_urls = set(self.override_discovered_urls)
        else:
            sitemap_urls = await self.discover_sitemap_urls()
```

### 4. Streamlit Dashboard (app.py)
```python
def display_pattern_selection():
    """Display URL pattern selection interface"""
    for pattern, data in st.session_state.url_patterns.items():
        col1, col2, col3, col4 = st.columns([0.5, 2, 1, 2])
        with col1:
            if st.checkbox("", key=f"pattern_{hash(pattern)}"):
                selected_patterns.add(pattern)
```

## Features in Detail

### URL Pattern Analysis
1. **Pattern Detection**
   - Numeric parameters -> {n}
   - UUIDs/hashes -> {id}
   - Dates -> {date}
   - Slugs -> {slug}

2. **Pattern Selection**
   - Checkbox selection
   - Count display
   - Example URLs
   - Total URL counter

3. **Memory Management**
   - Batch processing
   - Memory monitoring
   - Automatic pausing
   - Resource cleanup

4. **History Tracking**
   - Domain statistics
   - Success rates
   - Memory graphs
   - Timeline views

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

1. **URL Analysis and Crawling**
   - Enter website URL
   - Click "Analyze Sitemap"
   - Select desired URL patterns
   - Start crawling

2. **History Viewing**
   - Navigate to History page
   - View domain statistics
   - Analyze success rates
   - Monitor memory usage

## Best Practices

1. **Pattern Selection**
   - Review patterns before crawling
   - Check example URLs
   - Monitor URL counts
   - Use test mode initially

2. **Memory Management**
   - Keep batch sizes reasonable
   - Monitor memory graphs
   - Set appropriate thresholds
   - Enable automatic pausing

3. **History Analysis**
   - Review past crawls
   - Check success rates
   - Monitor memory trends
   - Analyze failures

4. **Error Handling**
   - Check error messages
   - Review failed URLs
   - Adjust timeouts
   - Monitor resources

## Contributing

Feel free to submit issues and enhancement requests!

## License

MIT License - feel free to use in your own projects.