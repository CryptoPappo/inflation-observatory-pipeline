import os
import sys
import responses
from unittest.mock import MagicMock
from sqlalchemy import create_engine, select, inspect
from sqlalchemy.orm import sessionmaker

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

def make_mock_driver():
    driver = MagicMock()
    
    html_by_url = {
            "https://supermarket.com/productos/producto-1": "<html>producto 1</html>",
            "https://supermarket.com/productos/producto-2": "<html>producto 2</html>"
    }

    def get_side_effect(url):
        driver.page_source = html_by_url[url]

    driver.get.side_effect = get_side_effect

    return driver

@responses.activate
def test_coto_scrape():
    scraper = CotoScraper()
    
    mock_sitemap_data = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset>
        <url>
            <loc>https://supermarket.com/productos</loc>
        </url>
        <url>
            <loc>https://supermarket.com/categorias</loc>
        </url>
        <url>
            <loc>https://supermarket.com/marcas</loc>
        </url>
    </urlset>
    """
    responses.add(
        responses.GET,
        scraper.base_url,
        body=mock_sitemap_data,
        status=200,
        content_type="application/xml"
    )
    
    mock_products = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset>
        <url>
            <loc>https://supermarket.com/productos/producto-1</loc>
        </url>
        <url>
            <loc>https://supermarket.com/productos/producto-2</loc>
        </url>
    </urlset>
    """
    responses.add(
        responses.GET,
        "https://supermarket.com/productos",
        body=mock_products,
        status=200,
        content_type="application/xml"
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    mock_driver = make_mock_driver()
    Session = sessionmaker(bind=engine)
    scraper.scrape(Session, mock_driver)
    
    with Session() as session:
        rows = session.scalars(select(RawPages)).all()
        assert len(rows) == 2
        product_1 = rows[0]
        product_2 = rows[1]
        assert product_1.html != product_2.html
