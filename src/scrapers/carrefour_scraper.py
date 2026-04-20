import uuid
import re
import tenacity
import json
import ast
from datetime import datetime
from bs4 import BeautifulSoup
from requests_ratelimiter import LimiterSession

from src.utils.logging import get_logger
from src.utils.tools import safe_get
from src.scrapers.base import BaseScraper
logger = get_logger("carrefour_scraper")

class CarrefourScraper(BaseScraper):
    base_url = "https://www.carrefour.com.ar/sitemap.xml"
    store = "carrefour"

    def __init__(self, scrape_id: str):
        self.scrape_id = scrape_id
        
    def product_headers(self, product_url: str) -> dict[str, str]:
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

    def scrape(self) -> list[dict]:
        session = LimiterSession(
            per_second=1,
            per_minute=60
        )

        raw_responses = []
        try:
            response = safe_get(
                    session,
                    self.base_url,
                    timeout=10
            )
        except Exception as e:
            logger.exception("An error ocurred with carrefour sitemap call")
            raise
        logger.info("Finished downloading carrefour sitemap xml")
        raw_responses.append(
                {
                    "raw_id": uuid.uuid4().hex,
                    "scrape_id": self.scrape_id,
                    "store": self.store,
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
                response = safe_get(
                        session,
                        product_xml_url,
                        timeout=10
                )
            except Exception as e:
                logger.exception(f"An error ocurred with carrefour products.xml call: {product_xml_url}")
            else:
                logger.info("Finished downloading carrefour products xml")
                raw_responses.append(
                        {
                            "raw_id": uuid.uuid4().hex,
                            "scrape_id": self.scrape_id,
                            "store": self.store,
                            "url": product_xml_url,
                            "response_type": "xml",
                            "response_category": "products",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )
                soup = BeautifulSoup(response.text, "xml")
                products_urls.extend([product.text for product in soup.find_all("loc")])
        
        products_ids = []
        for product_url in products_urls[:10].copy():
            try:
                response = safe_get(
                        session,
                        product_url,
                        timeout=10
                )
            except Exception as e:
                logger.exception(f"An error ocurred with carrefour product call: {product_url}")
                products_urls.remove(product_url)
            else:
                logger.info(f"Downloaded carrefour source code of product: {product_url}")
                source_code = response.text
                match_ = re.search(r"\"productId\":\"[0-9]+\"", source_code)
                if match_ is None:
                    continue
                else:
                    product_id_match = match_.group(0)
                    product_id = product_id_match.split("\"")[-2]
                    products_ids.append(product_id)
                    raw_responses.append(
                            {
                                "raw_id": uuid.uuid4().hex,
                                "scrape_id": self.scrape_id,
                                "store": self.store,
                                "url": product_url,
                                "response_type": "html",
                                "response_category": "product",
                                "payload": source_code,
                                "time": datetime.utcnow()
                            }
                    )
            
        api_url = "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:"
        for product_id, product_url in zip(products_ids, products_urls[:len(products_ids)]):
            headers = self.product_headers(product_url)
            url = api_url + product_id
            try:
                response = safe_get(
                        session,
                        url,
                        headers=headers,
                        timeout=10
                )
            except Exception as e:
                logger.exception(f"An error ocurred with carrefour product call: {product_url}")
            else:
                logger.info(f"Downloaded carrefour product json: {url}")
                raw_responses.append(
                        {
                            "raw_id": uuid.uuid4().hex,
                            "scrape_id": self.scrape_id,
                            "store": self.store,
                            "url": url,
                            "response_type": "json",
                            "response_category": "product",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )
        
        return raw_responses

    def parse(self, raw_data: str) -> dict:
        raw_json = json.loads(raw_data)[0]
        
        raw_prices = raw_json["items"][0]["sellers"][0]["commertialOffer"]
        
        raw_discount = raw_prices.get("PromotionTeasers", [])
        if len(raw_discount) == 0:
            raw_discount = {"name": ""}
        else:
            raw_discount = raw_discount[0]
        
        raw_categories = raw_json.get("categories", ["//"])[0].split("/")
        try:
            category = raw_categories[1]
        except IndexError:
            category = ""
        try:
            subcategory = raw_categories[-2]
        except IndexError:
            subcategory = ""

        return {
                "name": raw_json["productName"],
                "sku": raw_json.get("productId", "0"),
                "ean": raw_json["EAN"][0],
                "category": category,
                "subcategory": subcategory,
                "brand": raw_json.get("brand", ""),
                "unit": raw_json.get("Gramaje leyenda de conversión", [""])[0],
                "regular_price": raw_prices["ListPrice"],
                "discount_price": raw_prices.get("Price", raw_prices["ListPrice"]),
                "unit_price": raw_json.get("pricePerUnit", "0"),
                "untaxed_price": 0,
                "discount": raw_discount.get("name", "")
        }
