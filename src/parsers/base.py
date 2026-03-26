from abc import ABC, abstractmethod
from bs4 import BeautifulSoup

def get_html_attr(
        soup: BeautifulSoup,
        element: str,
        class_: str
) -> str | None:
    try:
        attribute = soup.find(element, class_=class_).text
    except AttributeError:
        attribute = None
    
    return attribute

class BaseParser(ABC):
    @abstractmethod
    def parse(self, html: str) -> dict:
        """Extract raw fields from HTML"""
