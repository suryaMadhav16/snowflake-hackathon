import pytest
from bs4 import BeautifulSoup
from app.core.url_discovery import URLDiscoveryManager

@pytest.mark.asyncio
class TestURLDiscoveryManager:
    """Test URL discovery functionality"""

    def test_initialization(self):
        """Test manager initialization"""
        excluded_patterns = [r".*\.pdf$", r"/api/.*"]
        manager = URLDiscoveryManager(max_depth=3, excluded_patterns=excluded_patterns)
        
        assert manager.max_depth == 3
        assert len(manager.excluded_patterns) == 2
        assert manager.discovered_urls == set()
        assert manager._lock is not None

    def test_url_validation(self):
        """Test URL validation logic"""
        manager = URLDiscoveryManager(excluded_patterns=[r".*\.pdf$", r"/api/.*"])
        base_domain = "example.com"
        
        # Valid URLs
        assert manager._is_valid_url("https://example.com/page", base_domain)
        assert manager._is_valid_url("https://example.com/blog/post", base_domain)
        
        # Invalid URLs
        assert not manager._is_valid_url("https://other.com/page", base_domain)
        assert not manager._is_valid_url("https://example.com/doc.pdf", base_domain)
        assert not manager._is_valid_url("https://example.com/api/data", base_domain)
        assert not manager._is_valid_url("javascript:void(0)", base_domain)
        assert not manager._is_valid_url("#section", base_domain)
        assert not manager._is_valid_url("", base_domain)

    def test_url_extraction(self):
        """Test URL extraction from HTML"""
        manager = URLDiscoveryManager()
        base_url = "https://example.com"
        
        html = """
        <html>
            <body>
                <a href="/page1">Page 1</a>
                <a href="https://example.com/page2">Page 2</a>
                <a href="https://other.com/page3">External Page</a>
                <img src="/image.jpg">
                <script src="/script.js"></script>
                <a href="javascript:void(0)">Invalid</a>
                <a href="#section">Section</a>
            </body>
        </html>
        """
        
        urls = manager._extract_urls(html, base_url)
        
        assert "https://example.com/page1" in urls
        assert "https://example.com/page2" in urls
        assert "https://example.com/image.jpg" in urls
        assert "https://example.com/script.js" in urls
        assert "https://other.com/page3" not in urls
        assert "javascript:void(0)" not in urls
        assert "#section" not in urls

    async def test_discover_urls(self, mock_crawler):
        """Test URL discovery process"""
        manager = URLDiscoveryManager(max_depth=2)
        
        # Mock crawler responses
        mock_crawler.arun.side_effect = [
            type("CrawlResult", (), {
                "success": True,
                "html": """
                <html>
                    <a href="/page1">Page 1</a>
                    <a href="/page2">Page 2</a>
                </html>
                """,
                "url": "https://example.com"
            }),
            type("CrawlResult", (), {
                "success": True,
                "html": """
                <html>
                    <a href="/page3">Page 3</a>
                </html>
                """,
                "url": "https://example.com/page1"
            }),
            type("CrawlResult", (), {
                "success": True,
                "html": "<html></html>",
                "url": "https://example.com/page2"
            })
        ]
        
        # Run discovery
        result = await manager.discover_urls(
            "https://example.com",
            mode="full"
        )
        
        # Verify results
        assert len(result["urls"]) == 3
        assert result["total"] == 3
        assert result["max_depth"] == 2
        assert len(result["graph"]["nodes"]) == 3
        assert len(result["graph"]["links"]) >= 2

    async def test_quick_mode(self, mock_crawler):
        """Test quick discovery mode"""
        manager = URLDiscoveryManager()
        
        # Mock crawler to return many URLs
        mock_crawler.arun.return_value = type("CrawlResult", (), {
            "success": True,
            "html": "".join([
                f'<a href="/page{i}">Page {i}</a>'
                for i in range(200)
            ]),
            "url": "https://example.com"
        })
        
        result = await manager.discover_urls(
            "https://example.com",
            mode="quick"
        )
        
        # Quick mode should limit results
        assert len(result["urls"]) <= 100
        assert result["total"] <= 100

    async def test_error_handling(self, mock_crawler):
        """Test error handling during discovery"""
        manager = URLDiscoveryManager()
        
        # Mock crawler to fail
        mock_crawler.arun.side_effect = Exception("Crawl failed")
        
        with pytest.raises(Exception) as exc:
            await manager.discover_urls("https://example.com")
        assert str(exc.value) == "URL discovery error: Crawl failed"

    async def test_depth_limit(self, mock_crawler):
        """Test max depth enforcement"""
        manager = URLDiscoveryManager(max_depth=1)
        
        # Mock first level response with links
        mock_crawler.arun.return_value = type("CrawlResult", (), {
            "success": True,
            "html": """
            <html>
                <a href="/level1">Level 1</a>
                <a href="/level1/page2">Level 1 Page 2</a>
            </html>
            """,
            "url": "https://example.com"
        })
        
        result = await manager.discover_urls("https://example.com")
        
        # Should only process first level
        assert result["max_depth"] == 1
        for node in result["graph"]["nodes"]:
            assert node["depth"] <= 1

    def test_invalid_regex_patterns(self):
        """Test handling of invalid regex patterns"""
        # Invalid pattern should be skipped
        manager = URLDiscoveryManager(excluded_patterns=["[invalid", ".*\.pdf$"])
        assert len(manager.excluded_patterns) == 1
        
        # Valid URLs should still work
        assert manager._is_valid_url(
            "https://example.com/page",
            "example.com"
        )
        assert not manager._is_valid_url(
            "https://example.com/doc.pdf",
            "example.com"
        )