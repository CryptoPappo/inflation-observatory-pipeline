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
from src.models.raw_tables import Base, RawResponses, NormalizedResponses

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

    mock_product_disc = """
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
                }
            }]
        }]
    }
    """               
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-1?format=json",
        body=mock_product_disc,
        status=200,
        content_type="application/json"
    )
    
    mock_product_no_disc = """
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
                }
            }]
        }]
    }                                                            
    """
    responses.add(
        responses.GET,
        "https://supermarket.com/productos/producto-2?format=json",
        body=mock_product_no_disc,
        status=200,
        content_type="application/json"
    )

    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    scraper.scrape(Session)
    
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

        raw_scrape_ids = {row[0].scrape_id for row in rows_raw_json}
        normalized_scrape_ids = {row[0].scrape_id for row in rows_normalized}
        assert raw_scrape_ids == normalized_scrape_ids
    
def test_coto_parser():
    mock_product_disc = """
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
                }
            }]
        }]
    }
    """               
    
    mock_product_no_disc = """
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
                }
            }]
        }]
    }                                                            
    """

    parser = CotoScraper()
    product_disc = parser.parse(mock_product_disc)
    product_no_disc = parser.parse(mock_product_no_disc)

    assert product_disc["discount_price"] == "$1000"
    assert product_disc["discount"] == "50%"
    assert product_no_disc["discount_price"] == ""
    assert product_no_disc["discount"] == ""

    assert product_disc["regular_price"] == 2000.0
    assert product_disc["unit_price"] == 2500.0
    assert product_disc["untaxed_price"] == 1750.0
