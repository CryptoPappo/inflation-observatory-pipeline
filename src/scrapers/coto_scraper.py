import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options

from src.scrapers.base import BaseScraper
from src.utils.drivers import set_up

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

    def scrape(self):
        products_urls = self._get_products_urls()
        
        products = []
        for products_url in products_urls:
            response = requests.get(products_url, timeout=5)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "xml")
            products.extend([product.text for product in soup.find_all("loc")])
        
        #Build raw tables to insert to
        driver = set_up()
        for product in products[:2]:
            driver.get(product)
            html = driver.page_source


if __name__ == "__main__":
    scraper = CotoScraper()
    scraper.scrape()
