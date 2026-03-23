from abc import ABC, abstractmethod

class BaseScraper(ABC):

    @abstractmethod
    def _get_products_urls(self) -> list[str]:
        """Get latest products.xml urls from sitemap.xml"""

    @abstractmethod
    def scrape(self) -> None:
        """Render pages and store raw HTML / responses"""
