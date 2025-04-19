import sys
from pathlib import Path
import pytest
import httpx
from typing import AsyncGenerator, Generator, LiteralString

# Add project root to Python path
root_dir = str(Path(__file__).parent.parent.resolve())
sys.path.insert(0, root_dir)

# Configure pytest-asyncio
pytest_plugins = ("pytest_asyncio",)


@pytest.fixture
async def http_client() -> AsyncGenerator[httpx.AsyncClient, None]:
    """Fixture that creates a test httpx client."""
    async with httpx.AsyncClient() as client:
        yield client


@pytest.fixture
def sample_html() -> LiteralString:
    """Sample HTML content for testing parsers."""
    return """
    <html>
        <head><title>Test Page</title></head>
        <body>
            <div class="content">
                <a href="http://example.com">Example Link</a>
                <p class="text">Sample Text</p>
            </div>
        </body>
    </html>
    """

