import os
import sys
import argparse
import asyncio
import logging
from pathlib import Path
from urllib.parse import urlparse
from datetime import datetime

from ..crawler.parallel_crawler import ParallelCrawler

# Set up logging
def setup_logging(output_dir: Path, debug: bool = True):
    """Configure logging to both file and console"""

    # Create logs directory
    log_dir = output_dir / 'logs'
    log_dir.mkdir(parents=True, exist_ok=True)

    # Create log filename with timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f'crawler_{timestamp}.log'

    # Set logging level
    level = logging.DEBUG if debug else logging.INFO

    # Configure logging format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(level)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

    logging.info(f"Logs will be saved to: {log_file}")

def validate_url(url: str) -> bool:
    """Validate URL format"""
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc])
    except Exception:
        return False

def get_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(
        description='Crawl documentation websites and process with Groq'
    )

    parser.add_argument(
        'url',
        type=str,
        help='Base URL of the documentation website to crawl'
    )

    parser.add_argument(
        '--test',
        action='store_true',
        help='Run in test mode (limit to 5 pages)'
    )

    parser.add_argument(
        '--max-concurrent',
        type=int,
        default=5,
        help='Maximum number of concurrent crawls (default: 5)'
    )

    parser.add_argument(
        '--output-dir',
        type=str,
        default=str(Path(__file__).parent.parent.parent / 'data'),
        help='Output directory for crawled data'
    )

    parser.add_argument(
        '--debug',
        action='store_true',
        default=True,  # Set debug to True by default
        help='Enable debug logging'
    )

    return parser.parse_args()

async def main():
    args = get_args()

    # Convert output_dir to Path
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    setup_logging(output_dir, debug=args.debug)

    # Log startup information
    logging.info(f"Starting crawler with configuration:")
    logging.info(f"URL: {args.url}")
    logging.info(f"Test mode: {args.test}")
    logging.info(f"Max concurrent: {args.max_concurrent}")
    logging.info(f"Output directory: {output_dir}")
    logging.info(f"Debug mode: {args.debug}")

    # Validate URL
    if not validate_url(args.url):
        logging.error(f"Invalid URL format: {args.url}")
        sys.exit(1)

    # Check GROQ API key
    groq_api_key = os.getenv('GROQ_API_KEY')
    if not groq_api_key:
        logging.error("GROQ_API_KEY environment variable not set")
        sys.exit(1)

    try:
        # Initialize crawler
        logging.info("Initializing crawler...")
        print("-----------------Groq API KEY--------------")
        print(groq_api_key)
        print("----------------------------------")
        crawler = ParallelCrawler(
            base_url=args.url,
            output_dir=str(output_dir),
            groq_api_key=groq_api_key,
            max_concurrent=args.max_concurrent,
            test_mode=args.test
        )

        # Start crawling
        logging.info("Starting crawl...")
        await crawler.crawl()

    except KeyboardInterrupt:
        logging.info("\nCrawling interrupted by user")
    except Exception as e:
        logging.error(f"Error during crawling: {str(e)}", exc_info=True)
    finally:
        logging.info("Crawling completed")

if __name__ == "__main__":
    asyncio.run(main())
