from abc import ABC, abstractmethod

class BaseParser(ABC):
    @abstractmethod
    def parse(self, raw_html: str) -> list[dict]:
        """Extract raw fields from HTML"""
