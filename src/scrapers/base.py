from abc import ABC, abstractmethod

class BaseScraper(ABC):

    @property
    @abstractmethod
    def store(self) -> str:
        """Unique store identifier (e.g. 'coto', 'carrefour')"""

    @property
    @abstractmethod
    def base_url(self) -> str:
        """Base URL or entry point for the scraper"""

    @abstractmethod
    def product_headers(self, product_url: str) -> dict[str, str]:
        """Headers to get products raw json"""

    @abstractmethod
    def scrape(self) -> list[dict]:
        """Render pages and returns raw HTML / responses"""

    @abstractmethod
    def parse(self, raw_data: str) -> dict:
        """Normalize raw jsons"""
