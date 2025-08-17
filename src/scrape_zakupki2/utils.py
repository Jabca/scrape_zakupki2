import random
import time
from typing import Any, Optional
from loguru import logger

import requests


class RequestSessioner:
    """Simple web scraping client without context manager"""

    # Good default headers for web scraping
    DEFAULT_HEADERS: dict[str, str] = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.5",
        "Referer": "https://www.google.com/",
        "DNT": "1",
    }

    def __init__(
        self,
        headers: Optional[dict[str, str]] = None,
        delay: float = 0.5,
        tries: int = 10,
        timeout: float = 10,
    ):
        """
        Create a new scraper instance

        :param headers: Custom headers (will be merged with defaults)
        :param delay: Minimum seconds between requests (helps avoid bans)
        :param timeout: Request timeout in seconds
        """
        self.session = requests.Session()
        self.delay = delay
        self.timeout = timeout
        self.last_request_time = 0

        # Set headers
        final_headers = self.DEFAULT_HEADERS.copy()
        if headers:
            final_headers.update(headers)
        self.session.headers.update(final_headers)

    def _wait_if_needed(self):
        """Enforce delay between requests with slight randomization"""
        if self.delay > 0:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.delay:
                # Add small random variation to delay pattern
                wait_time = self.delay - elapsed + random.uniform(0, 0.5)
                time.sleep(wait_time)
        self.last_request_time = time.time()

    def __check_raise_404(self, html: str):
        """
        cause this fuckers don't return 404 as 200
        """
        ERROR_HTML = "<html>\r\n<head><title>404 Not Found</title"

        if html[: len(ERROR_HTML)] == ERROR_HTML:
            raise requests.exceptions.RequestException(
                "404 error (probably ddos protection)"
            )

    def _get(self, url: str, params: Any = None, tries: int = 5) -> requests.Response:
        """Get HTML content from URL"""
        error: Exception = Exception("Wasn't able to make any requests, check params")
        for _ in range(tries):
            try:
                self._wait_if_needed()
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                html = response.text
                self.__check_raise_404(html)
                return response
            except Exception as e:
                error = e
                logger.error(f"Request to {url} failed: {e}")
        raise error

    def get_text(self, url: str, params: Any = None) -> str:
        return self._get(url=url, params=params).text

    def get_content(self, url: str, params: Any = None) -> bytes | None:
        try:
            return self._get(url=url, params=params).content
        except Exception as e:
            logger.error(f"Request to {url} failed because of {e}")
            return None

    def close(self):
        """Close the session when done"""
        self.session.close()
