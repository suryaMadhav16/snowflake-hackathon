# Documentation Website Crawler

A high-performance crawler that extracts content from technical documentation websites and saves it as markdown files, including images.

## Features

- **Parallel Web Crawling**
  - Configurable concurrent crawling
  - Smart rate limiting
  - Resume functionality for interrupted crawls
  - Test mode for quick validation

- **Content Processing**
  - Markdown conversion with structural preservation
  - Automatic image downloading and organization
  - Clean directory hierarchy
  - URL discovery and tracking

## Prerequisites

- Python 3.9+
- Internet connection
- Sufficient storage space

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/snowflake-hackathon.git
cd snowflake-hackathon
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

### Basic Crawling
```bash
python -m src.cli.main https://docs.example.com
```

### Test Mode (Limited to 5 pages)
```bash
python -m src.cli.main https://docs.example.com --test
```

### Advanced Options
```bash
python -m src.cli.main https://docs.example.com \
  --max-concurrent 10 \
  --output-dir ./custom_output \
  --rate-limit 2.0 \
  --debug
```

## Directory Structure

```
data/
├── raw/                # Raw markdown files
│   └── {domain}/
│       └── {hash}/
│           └── content.md
├── images/            # Downloaded images
│   └── {domain}/
│       └── {hash}/
│           └── images/
└── logs/             # Crawler logs
    └── crawler_{timestamp}.log
```

## Command Line Arguments

| Argument | Description | Default |
|----------|-------------|---------|
| url | Website URL to crawl | Required |
| --test | Limit crawl to 5 pages | False |
| --max-concurrent | Maximum concurrent crawls | 5 |
| --rate-limit | Requests per second | 2.0 |
| --output-dir | Custom output directory | ./data |
| --debug | Enable debug logging | True |

## Error Handling

- Automatic retry for failed requests
- Comprehensive error logging
- Resume capability for interrupted crawls
- Rate limit respect
- Resource cleanup

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- crawl4ai library for web crawling capabilities
- Beautiful Soup for HTML processing
