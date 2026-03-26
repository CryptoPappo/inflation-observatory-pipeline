from bs4 import BeautifulSoup

from src.parser.base import BaseParser, get_html_attr

class CotoParser(BaseParser):
    def parse(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        
        return {
                "ean": get_html_attr(soup, "div", "rating-wrap my-3"),
                "name": get_html_attr(soup, "h2", "title"),
                "category": get_html_attr(soup, "div", "col-12"),
                "discount_price": get_html_attr(soup, "div", "mb-1"),
                "regular_price": get_html_attr(soup, "div", "mt-2"),
                "unit_price": get_html_attr(soup, "div", "mb-5"),
                "discount": get_html_attr(soup, "div", "mb-2 ng-star-inserted")
        }



    
