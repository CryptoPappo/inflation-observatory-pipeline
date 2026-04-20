import os
import sys
import responses
import requests
from datetime import datetime
from unittest.mock import MagicMock
from sqlalchemy import create_engine, select, inspect
from sqlalchemy.orm import sessionmaker

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(parent_dir)

from src.scrapers.coto_scraper import CotoScraper
from src.models.raw_tables import Base, RawResponses, NormalizedResponses
from src.loaders.load_raw_data import load_raw_responses, load_normalized_responses

def make_mock_product_json_discount():
    return """
    {"contents":
        [{"Main":
            [{"record":
                {"attributes":
                    {"product.displayName": ["Leche Serenisima"],
                     "sku.repositoryId": ["sku00231560"],
                     "product.eanPrincipal": ["77283193095"],
                     "allAncestors.displayName": ["Lacteos", "Leches Enteras"],
                     "sku.unit_of_measure": ["litres"],
                     "product.dtoDescuentos": ["[{'precioDescuento': '$1000', 'textoDescuento': '50%'}]"],
                     "sku.dtoPrice": ["{'precioLista': 2000.0, 'precio': 2500.0, 'precioSinImp': 1750.0}"]
                    }
                },
            "breadcrumbsConstructor":
                [{"label": "Inicio"}, {"label": "Almacen"}, {"label": "Leche"}]
            }]
        }]
    }
    """ 

def make_mock_product_json_no_discount():
    return """
    {"contents":
        [{"Main":
            [{"record":
                {"attributes":
                    {"product.displayName": ["Leche Serenisima"],
                     "sku.repositoryId": ["sku00231560"],
                     "product.eanPrincipal": ["77283193095"],
                     "allAncestors.displayName": ["Lacteos", "Leches Enteras"],
                     "sku.unit_of_measure": ["litres"],
                     "product.dtoDescuentos": ["[]"],
                     "sku.dtoPrice": ["{'precioLista': 2000.0, 'precio': 2500.0, 'precioSinImp': 1750.0}"]
                    }
                },
            "breadcrumbsConstructor": 
                [{"label": "Inicio"}, {"label": "Almacen"}, {"label": "Leche"}]
            }]
        }]
    }                                                            
    """

@responses.activate
def test_coto_scrape():
    scrape_id = datetime.utcnow().strftime("%Y-%m-%d")
    scraper = CotoScraper(scrape_id)
    
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

    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-1?format=json",
        body=make_mock_product_json_discount(),
        status=200,
        content_type="application/json"
    )
    
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-2?format=json",
        body=make_mock_product_json_no_discount(),
        status=200,
        content_type="application/json"
    )
    
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-3?format=json",
        body=requests.exceptions.HTTPError("HTTPError")
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    
    raw_responses = scraper.scrape()
    load_raw_responses(raw_responses, Session)
    
    normalized_responses = []
    for raw_response in raw_responses:
        if raw_response["response_category"] != "product":
            continue
        else:
            normalized_responses.append(
                    {
                        "raw_id": raw_response["raw_id"],
                        "normalized_payload": scraper.parse(raw_response["payload"])
                    }
            )
    load_normalized_responses(normalized_responses, Session)

    with Session() as session:
        rows_raw = session.execute(select(RawResponses)).all()
        rows_normalized = session.execute(select(NormalizedResponses)).all()
        assert len(rows_raw) == 4
        assert len(rows_normalized) == 2
       
        rows_raw_json = session.execute(
                select(RawResponses)
                .where(RawResponses.response_type == "json")
                .where(RawResponses.response_category == "product")
        ).all()

        raw_scrape_ids = {row[0].raw_id for row in rows_raw_json}
        normalized_scrape_ids = {row[0].raw_id for row in rows_normalized}
        assert raw_scrape_ids == normalized_scrape_ids
    
def test_coto_parser():
    scrape_id = datetime.utcnow().strftime("%Y-%m-%d")
    parser = CotoScraper(scrape_id)
    product_disc = parser.parse(make_mock_product_json_discount())
    product_no_disc = parser.parse(make_mock_product_json_no_discount())

    assert product_disc["discount_price"] == "$1000"
    assert product_disc["discount"] == "50%"
    assert product_no_disc["discount_price"] == "2000.0"
    assert product_no_disc["discount"] == ""

    assert product_disc["regular_price"] == 2000.0
    assert product_disc["unit_price"] == 2500.0
    assert product_disc["untaxed_price"] == 1750.0
