import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.raw_tables import Base
from src.scrapers.coto_scraper import CotoScraper
from src.scrapers.carrefour_scraper import CarrefourScraper
from src.utils.logging import get_logger
logger = get_logger("main")

def main():
    logger.info("Initializing scraping and parsing scripts...")

    success = load_dotenv()
    if not success:
        logger.error("An error ocurred loading .env file")
        raise RuntimeError

    db_url = os.getenv("DATABASE_URL")
    if db_url is None:
        logger.error("An eror ocurred loading database url from .env file")
        raise RuntimeError

    engine = create_engine(db_url)
    session = sessionmaker(bind=engine)

    Base.metadata.create_all(engine)

    scrapers_by_store = {
            "coto": CotoScraper(),
            "carrefour": CarrefourScraper()
    }
    for store, scraper in scrapers_by_store:
        try:
            scraper.scrape(session)
        except Exception as e:
            logger.exception(f"An error ocurred scraping store {store}") 
