import requests
import json
import ast
from datetime import datetime
from bs4 import BeautifulSoup
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from src.utils.logging import get_logger
from src.scrapers.base import BaseScraper
from src.models.raw_tables import RawResponses
logger = get_logger("carrefour_scraper")

class CarrefourScraper(BaseScraper):
    @property
    def base_url(self) -> str:
        return "https://www.carrefour.com.ar/sitemap.xml"
    
    def product_headers(self, product_url: str) -> str:
        return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
                "Accept": "*/*",
                "Accep-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Referer": product_url,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "Connection": "keep-alive"
        }

    def scrape(self, Session: sessionmaker):
        raw_responses = []
        try:
            response = requests.get(self.base_url, timeout=10)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.exception("An error ocurred with carrefour sitemap call")
            raise
        raw_responses.append(
                {
                    "store": "carrefour",
                    "url": self.base_url,
                    "response_type": "xml",
                    "response_category": "sitemap",
                    "payload": response.text,
                    "time": datetime.utcnow()
                }
        )

        soup = BeautifulSoup(response.text, "xml")
        urls = soup.find_all("loc")
        products_xml_urls = []
        for url in urls:
            if "product" in url.text:
                products_xml_urls.append(url.text)

        products_urls = []
        for product_xml_url in products_xml_urls:
            try:
                response = requests.get(product_xml_url, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.exception(f"An error ocurred with carrefour products.xml call: {products_url}")
            else:
                raw_responses.append(
                        {
                            "store": "carrefour",
                            "url": product_xml_url,
                            "response_type": "xml",
                            "response_category": "products",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )
                soup = BeautifulSoup(response.text, "xml")
                products_urls.extend([product.text for product in soup.find_all("loc")])
        
        api_url = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:"
        products_ids = [product_url.split("/")[-2].split("-")[-1] for product_url in products_urls]
        for product_id, product_url in zip(products_ids, products_urls):
            headers = self.product_headers(product_url)
            url = api_url + product_id
            try:
                response = requests.get(url, headers=headers, timeout=10)
                response.raise_for_status()
            except requests.exceptions.RequestException as e:
                logger.exception(f"An error ocurred with carrefour product call: {product_url}")
            else:
                raw_responses.append(
                        {
                            "store": "carrefour",
                            "url": url,
                            "response_type": "json",
                            "response_category": "product",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )
        
        stmt = insert(RawResponses).values(raw_responses)
        stmt = stmt.on_conflict_do_nothing(
                index_elements=[RawResponses.scrape_id]
        )
        with Session() as session:
            result = session.execute(stmt)
            inserted = result.rowcount
            session.commit()
        
        logger.info(f"Raw responses insert: attempted={len(raw_responses)} inserted={inserted}")

    def parse(self, raw_data: str) -> dict:
        raw_json = json.loads(raw_data)[0]

        return {
                "name": raw_json.get("productName", ""),
                "sku": raw_json.get("productId", ""),
                "ean": raw_json.get("EAN", [""])[0],
                "category": raw_json.get("categories", [""])[0],
                "unit": raw_json.get("Gramaje leyenda de conversión", [""])[0],
                "discount_price": ra
