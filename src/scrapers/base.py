from abc import ABC, abstractmethod
from selenium.webdriver.firefox.webdriver import WebDriver
from sqlalchemy.orm.session import sessionmaker

class BaseScraper(ABC):
    @property
    @abstractmethod
    def base_url(self) -> str:
        """Base URL or entry point for the scraper"""

    @propery
    @abstractmethod
    def product_headers(self, product_url: str) -> str:
        """Headers to get products raw json"""

    @abstractmethod
    def scrape(self, Session: sessionmaker) -> None:
        """Render pages and store raw HTML / responses"""
