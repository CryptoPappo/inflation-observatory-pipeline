import requests
from bs4 import BeautifulSoup

from .base import BaseScraper

class CotoScraper(BaseScraper):
    def __init__(self):
        self.sitemap_url = "https://www.cotodigital.com.ar/sitios/cdigi/sitemap.xml"

    def get_products_urls(self):
        site = requests.get(self.sitemap_url)
        soup = BeautifulSoup(site.text, "xml")
        urls = soup.find_all("loc")
        
        products_urls = []
        for url in urls:
            if "producto" in url.text:
                products_urls.append(url.text)

        return products_urls

    def scrape(self):
        products_urls = self.get_products_urls()        
