from abc import ABC, abstractmethod
from bs4 import BeautifulSoup

def get_html_attr(
        soup: BeautifulSoup,
        element: str,
        class_: str,
        keywords: list[str] = [""]
) -> str:
    attributes = soup.find_all(element, class_=class_)
    attrs = ""
    for attribute in attributes:
        if any([keyword in attribute.text for keyword in keywords]):
            attrs = attrs + " " + attribute.text

    return attrs

class BaseParser(ABC):
    @abstractmethod
    def parse(self, html: str) -> dict:
        """Extract raw fields from HTML"""
