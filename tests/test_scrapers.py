import os
import sys
import responses
from sqlalchemy import create_engine, inspect

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

from src.scrapers.coto_scraper import CotoScraper
from src.models.raw_tables import (
        Base,
        RawPages,
        RawProducts
)



def test_tables_created():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert "raw_pages" in tables
    assert "raw_products" in tables

@responses.activate
def test_coto_scrape():
    scraper = CotoScraper()
    
    mock_sitemap_data = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset>
        <url>
            <loc>https://supermarket.com/productos/1</loc>
        </url>
        <url>
            <loc>https://supermarket.com/productos/2</loc>
        </url>
        <url>
            <loc>https://supermarket.com/marcas</loc>
        </url>
    </urlset>
    """
    responses.add(
        responses.GET,
        scraper.sitemap_url,
        body=mock_sitemap_data,
        status=200,
        content_type="application/xml"
    )





