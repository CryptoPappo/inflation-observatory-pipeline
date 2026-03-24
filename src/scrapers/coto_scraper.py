import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from src.utils.logging import get_logger
from src.scrapers.base import BaseScraper
from src.utils.drivers import set_up
from src.models.raw_tables import RawPages, RawProducts
logger = get_logger("coto_scraper")

class CotoScraper(BaseScraper):
    def __init__(self):
        self.sitemap_url = "https://www.cotodigital.com.ar/sitios/cdigi/sitemap.xml"

    def _get_products_urls(self):
        response = requests.get(self.sitemap_url, timeout=5)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "xml")
        urls = soup.find_all("loc")
        
        products_urls = []
        for url in urls:
            if "producto" in url.text:
                products_urls.append(url.text)

        return products_urls

    def scrape(self, Session: sessionmaker):
        products_urls = self._get_products_urls()
        
        products = []
        for products_url in products_urls:
            response = requests.get(products_url, timeout=5)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "xml")
            products.extend([product.text for product in soup.find_all("loc")])
        
        raw_htmls = []
        driver = set_up()
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
            result = sessionn.execute(stmt)
            inserted = result.rowcount
            session.commit()
        
        logger.info(f"Raw pages insert: attempted={len(raw_htmls)} inserted={inserted}")

