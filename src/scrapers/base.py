from abc import ABC, abstractmethod
from selenium.webdriver.firefox.webdriver import WebDriver
from sqlalchemy.orm.session import sessionmaker

class BaseScraper(ABC):
    @property
    @abstractmethod
    def base_url(self) -> str:
        """Base URL or entry point for the scraper."""

    @abstractmethod
    def _get_products_urls(self) -> list[str]:
        """Get latest products.xml urls from sitemap.xml"""

    @abstractmethod
    def scrape(
            self,
            Session: sessionmaker,
            driver: WebDriver
    ) -> None:
        """Render pages and store raw HTML / responses"""
