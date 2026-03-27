import requests
from datetime import datetime
from bs4 import BeautifulSoup
from selenium.webdriver.firefox.webdriver import WebDriver
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from src.utils.logging import get_logger
from src.scrapers.base import BaseScraper
from src.models.raw_tables import RawPages, RawProducts
logger = get_logger("coto_scraper")

headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:149.0) Gecko/20100101 Firefox/149.0', 'Accept': 'application/json, text/plain, */*', 'Accept-Language': 'en-US,en;q=0.9', 'Accept-Encoding': 'gzip, deflate, br, zstd', 'Content-Type': 'application/json', 'Connection': 'keep-alive', 'Referer': 'https://www.cotodigital.com.ar/sitios/cdigi/productos/-gaseosa-coca-cola-zero-2,25-lt/_/R-00180416-00180416-200', 'Sec-Fetch-Dest': 'empty', 'Sec-Fetch-Mode': 'cors', 'Sec-Fetch-Site': 'same-origin', 'DNT': '1', 'Sec-GPC': '1'}

class CotoScraper(BaseScraper):
    @property
    def base_url(self) -> str:
        return "https://www.cotodigital.com.ar/sitios/cdigi/sitemap.xml"

    def _get_products_urls(self):
        response = requests.get(self.base_url, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "xml")
        urls = soup.find_all("loc")
        
        products_urls = []
        for url in urls:
            if "producto" in url.text:
                products_urls.append(url.text)

        return products_urls

    def scrape(
            self,
            Session: sessionmaker,
            driver: WebDriver
    ):
        products_urls = self._get_products_urls()
        
        products = []
        for products_url in products_urls:
            response = requests.get(products_url, timeout=5)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "xml")
            products.extend([product.text for product in soup.find_all("loc")])
        
        raw_htmls = []
        for product in products:
            driver.get(product)
            html = driver.page_source
            raw_htmls.append(
                    {
                        "store": "coto",
                        "url": product,
                        "html": html,
                        "time": datetime.utcnow()
                    }
            )

        stmt = insert(RawPages).values(raw_htmls)
        stmt = stmt.on_conflict_do_nothing(
                index_elements=[RawPages.scrape_id]
        )
        with Session() as session:
            result = session.execute(stmt)
            inserted = result.rowcount
            session.commit()
        
        logger.info(f"Raw pages insert: attempted={len(raw_htmls)} inserted={inserted}")

