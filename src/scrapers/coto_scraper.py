import uuid
import tenacity
import json
import ast
from datetime import datetime
from bs4 import BeautifulSoup
from requests_ratelimiter import LimiterSession

from src.utils.logging import get_logger
from src.utils.tools import safe_get
from src.scrapers.base import BaseScraper
logger = get_logger("coto_scraper")

class CotoScraper(BaseScraper):
    @property
    def base_url(self) -> str:
        return "https://www.cotodigital.com.ar/sitios/cdigi/sitemap.xml"

    def product_headers(self, product_url: str) -> dict[str, str]:
        return {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0",
                "Accept": "application/json, text/plain, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Content-Type": "application/json",
                "Connection": "keep-alive", 
                "Referer": product_url,
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin",
                "DNT": "1",
                "Sec-GPC": "1"
        }

    def scrape(self) -> list[dict]:
        scrape_id = datetime.utcnow().strftime("%Y-%m-%d_coto")

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
            logger.exception("An error ocurred with coto sitemap call")
            raise
        logger.info("Finished downloading coto sitemap xml")
        raw_responses.append(
                {
                    "raw_id": uuid.uuid4().hex,
                    "scrape_id": scrape_id,
                    "store": "coto",
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
            if "producto" in url.text:
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
                logger.exception(f"An error ocurred with coto products.xml call: {product_xml_url}")
            else:
                logger.info("Finished downloading coto products xml")
                raw_responses.append(
                        {
                            "raw_id": uuid.uuid4().hex,
                            "scrape_id": scrape_id,
                            "store": "coto",
                            "url": product_xml_url,
                            "response_type": "xml",
                            "response_category": "products",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )
                soup = BeautifulSoup(response.text, "xml")
                products_urls.extend([product.text for product in soup.find_all("loc")])

        for product_url in products_urls[:10]:
            headers = self.product_headers(product_url)
            product_url += "?format=json"
            try:
                response = safe_get(
                        session,
                        product_url,
                        headers=headers,
                        timeout=10
                )
            except Exception as e:
                logger.exception(f"An error ocurred with coto product call: {product_url}")
            else:
                logger.info(f"Donwloaded coto product: {product_url}")
                raw_responses.append(
                        {
                            "raw_id": uuid.uuid4().hex,
                            "scrape_id": scrape_id,
                            "store": "coto",
                            "url": product_url,
                            "response_type": "json",
                            "response_category": "product",
                            "payload": response.text,
                            "time": datetime.utcnow()
                        }
                )

        return raw_responses 
        
    def parse(self, raw_data: str) -> dict:
        raw_json = json.loads(raw_data)
        
        raw_attributes = raw_json["contents"][0]["Main"][0]["record"]["attributes"]

        raw_categories = raw_json["contents"][0]["Main"][0]["breadcrumbsConstructor"]
        try:
            category = raw_categories[1]["label"]
        except Exception as e:
            category = ""
        try:
            subcategory = raw_categories[-1]["label"]
        except Exception as e:
            subcategory = ""

        raw_prices = ast.literal_eval(raw_attributes["sku.dtoPrice"][0])
        regular_price = raw_prices["precioLista"]
        
        try:
            raw_discounts = ast.literal_eval(raw_attributes["product.dtoDescuentos"][0])[0]
        except IndexError:
            raw_discounts = {"precioDescuento": f"{regular_price}", "textoDescuento": ""}
        
        return {
                "name": raw_attributes["product.displayName"][0],
                "sku": raw_attributes.get("sku.repositoryId", ["0"])[0],
                "ean": raw_attributes["product.eanPrincipal"][0],
                "category": category,
                "subcategory": subcategory,
                "brand": raw_attributes.get("product.brand", [""])[0],
                "unit": raw_attributes.get("sku.unit_of_measure", [""])[0],
                "regular_price": regular_price,
                "discount_price": raw_discounts.get("precioDescuento", f"{regular_price}"),
                "unit_price": raw_prices.get("precio", 0),
                "untaxed_price": raw_prices.get("precioSinImp", 0),
                "discount": raw_discounts.get("textoDescuento", "")
        }
