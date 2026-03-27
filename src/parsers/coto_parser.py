from bs4 import BeautifulSoup

from src.parsers.base import BaseParser, get_html_attr

class CotoParser(BaseParser):
    def parse(self, html: str) -> dict:
        soup = BeautifulSoup(html, "html.parser")
        
        return {
                "ean": get_html_attr(soup, "div", "rating-wrap my-3", ["EAN"]),
                "name": get_html_attr(soup, "h2", "title text-dark fw-bolder"),
                "category": get_html_attr(soup, "div", "col-12 col"),
                "discount_price": get_html_attr(soup, "div", "mb-1", ["$"]),
                "regular_price": get_html_attr(soup, "div", "mt-2 small ng-star-inserted", ["$"]),
                "unit_price": get_html_attr(soup, "div", "mb-5 small ng-star-inserted", ["$"]),
                "untaxed_price": get_html_attr(soup, "div", "mb-2", ["$"]),
                "discount": get_html_attr(soup, "div", "mb-2 ng-star-inserted", ["%"]) + \
                        get_html_attr(soup, "div", "col-8", ["Oferta"])
        }



    
