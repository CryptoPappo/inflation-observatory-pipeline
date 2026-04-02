import os
import sys
import responses
import requests
from sqlalchemy import create_engine, select, inspect
from sqlalchemy.orm import sessionmaker

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

from src.scrapers.carrefour_scraper import CarrefourScraper
from src.models.raw_tables import Base, RawResponses

@responses.activate
def test_carrefour_scrape():
    scraper = CarrefourScraper()
   
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
         <url>
            <loc>https://supermarket.com/productos/producto-3</loc>
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

    mock_source_1 = """
    <!DOCTYPE html>
    <html lang="es-AR">
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta name="generator" content="vtex.render-server@8.179.1">
        <meta charset='utf-8'>

    {"name":"Leche","productId":"12345"}
    """
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-1",
        body=mock_source_1,
        status=200,
        content_type="text/html"
    )

    mock_source_1 = """
    <!DOCTYPE html>
    <html lang="es-AR">
    <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <meta name="generator" content="vtex.render-server@8.179.1">
        <meta charset='utf-8'>

    {"name":"Gaseosa","productId":"678910"}
    """
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-2",
        body=mock_source_1,
        status=200,
        content_type="text/html"
    )

    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-3",
        body=requests.exceptions.HTTPError("HTTPError")
    )

    mock_product_1 = """
    {
        'product_name': 'Leche',
        'price': '$1000.0',
        'unit': 'L',
        'ENA': '000001',
    }
    """
    responses.add(
        responses.GET,
        "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:12345",
        body=mock_product_1,
        status=200,
        content_type="application/json"
    )

    mock_product_2 = """
    {
        'product_name': 'Gaseosa',
        'price': '$4000.0',
        'unit': 'L',
        'ENA': '000002',
    }
    """
    responses.add(
        responses.GET,
        "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:678910",
        body=mock_product_2,
        status=200,
        content_type="application/json"
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    scraper.scrape(Session)
    
    with Session() as session:
        rows = session.scalars(select(RawResponses)).all()
        assert len(rows) == 6

def test_carrefour_parser():
    mock_product_disc = """
    [{"productName": "Leche",
      "productId": "12345",
       "EAN": ["7778794676"],
       "categories": ["Lacteos/Leches"],
       "Gramaje leyenda de conversión": ["Litre"],
       "pricePerUnit": "1000",
       "items": [{"sellers": [{"commertialOffer": {"Price": 500, "ListPrice": 1000, "PromotionTeasers": [{"name": "50%Dto"}]}}]}]
    }]
    """               
    mock_product_no_disc = """
    [{"productName": "Leche",
      "productId": "12345",
       "EAN": ["7778794676"],
       "categories": ["Lacteos/Leches"],
       "Gramaje leyenda de conversión": ["Litre"],
       "pricePerUnit": "1000",
       "items": [{"sellers": [{"commertialOffer":{"Price": 1000, "ListPrice": 1000, "PromotionTeasers": []}}]}]
    }]                                                          
    """

    parser = CarrefourScraper()
    product_disc = parser.parse(mock_product_disc)
    product_no_disc = parser.parse(mock_product_no_disc)

    assert product_disc["discount_price"] == 500
    assert product_disc["discount"] == "50%Dto"
    assert product_no_disc["discount_price"] == 1000
    assert product_no_disc["discount"] == ""

    assert product_disc["regular_price"] == 1000.0
    assert product_disc["unit_price"] == "1000"
    assert product_disc["untaxed_price"] == 0
