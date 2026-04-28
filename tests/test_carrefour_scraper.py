import uuid
import responses
import requests
from sqlalchemy import create_engine, select, inspect
from sqlalchemy.orm import sessionmaker

from price_history.scrapers.carrefour_scraper import CarrefourScraper
from price_history.models.raw_tables import Base, RawResponses, NormalizedResponses
from price_history.loaders.load_raw_data import load_raw_responses, load_normalized_responses

def make_mock_product_json_discount():
    return """
    [{"productName": "Leche",
      "productId": "12345",
       "EAN": ["7778794676"],
       "categories": ["/Almacen/Leche/"],
       "Gramaje leyenda de conversión": ["Litre"],
       "pricePerUnit": "1000",
       "items": [{"sellers": [{"commertialOffer": {"Price": 500, "ListPrice": 1000, "PromotionTeasers": [{"name": "50%Dto"}]}}]}]
    }]
    """               
def make_mock_product_json_no_discount():
    return """
    [{"productName": "Leche",
      "productId": "12345",
       "EAN": ["7778794676"],
       "categories": ["/Almacen/Leche/"],
       "Gramaje leyenda de conversión": ["Litre"],
       "pricePerUnit": "1000",
       "items": [{"sellers": [{"commertialOffer":{"Price": 1000, "ListPrice": 1000, "PromotionTeasers": []}}]}]
    }]                                                          
    """
@responses.activate
def test_carrefour_scrape():
    scrape_id = uuid.uuid4().hex
    scraper = CarrefourScraper(scrape_id=scrape_id)
   
    mock_sitemap_data = """<?xml version="1.0" encoding="UTF-8"?>
    <urlset>
        <url>
            <loc>https://supermarket.com/productos-1</loc>
        </url>
        <url>
            <loc>https://supermarket.com/productos-2</loc>
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
        "https://supermarket.com/productos-1",
        body=mock_products,
        status=200,
        content_type="application/xml"
    )

    responses.add(
        responses.GET,
        "https://supermarket.com/productos-2",
        body=requests.exceptions.HTTPError("HTTPError")
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

    mock_source_2 = """
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
        body=mock_source_2,
        status=200,
        content_type="text/html"
    )

    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-3",
        body=requests.exceptions.HTTPError("HTTPError")
    )

    responses.add(
        responses.GET,
        "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:12345",
        body=make_mock_product_json_discount(),
        status=200,
        content_type="application/json"
    )

    responses.add(
        responses.GET,
        "https://www.carrefour.com.ar/api/catalog_system/pub/products/search?fq=productId:678910",
        body=make_mock_product_json_no_discount(),
        status=200,
        content_type="application/json"
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    
    raw_iterator = scraper.scrape()
    raw_responses = list(raw_iterator)
    raw_rows_count = load_raw_responses(iter(raw_responses), Session)
    
    normalized_responses = []
    for raw_response in raw_responses:
        if raw_response["response_category"] != "product" or\
                raw_response["response_type"] != "json":
            continue
        else:
            normalized_responses.append(
                    {
                        "raw_id": raw_response["raw_id"],
                        "normalized_payload": scraper.parse(raw_response["payload"])
                    }
            )
    normalized_rows_count = load_normalized_responses(normalized_responses, Session)
    
    with Session() as session:
        rows_raw = session.execute(select(RawResponses)).all()
        rows_normalized = session.execute(select(NormalizedResponses)).all()
        assert len(rows_raw) == 6
        assert len(rows_raw) == raw_rows_count
        assert len(rows_normalized) == 2
        assert len(rows_normalized) == normalized_rows_count
       
        rows_raw_json = session.execute(
                select(RawResponses)
                .where(RawResponses.response_type == "json")
                .where(RawResponses.response_category == "product")
        ).all()

        raw_scrape_ids = {row[0].raw_id for row in rows_raw_json}
        normalized_scrape_ids = {row[0].raw_id for row in rows_normalized}
        assert raw_scrape_ids == normalized_scrape_ids

def test_carrefour_parser():
    parser = CarrefourScraper(scrape_id=None)
    product_disc = parser.parse(make_mock_product_json_discount())
    product_no_disc = parser.parse(make_mock_product_json_no_discount())

    assert product_disc["discount_price"] == 500
    assert product_disc["discount"] == "50%Dto"
    assert product_no_disc["discount_price"] == 1000
    assert product_no_disc["discount"] == ""

    assert product_disc["regular_price"] == 1000.0
    assert product_disc["unit_price"] == "1000"
    assert product_disc["untaxed_price"] == 0
