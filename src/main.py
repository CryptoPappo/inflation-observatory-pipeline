import os
import concurrent.futures
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from src.models.raw_tables import Base
from src.scrapers.coto_scraper import CotoScraper
from src.scrapers.carrefour_scraper import CarrefourScraper
from src.loaders.load_raw_data import load_raw_responses, load_normalized_responses
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
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        
        futures_to_store = {executor.submit(scraper.scrape): store for store, scraper in scrapers_by_store.items()}
        for future in concurrent.futures.as_completed(future_to_store):
            store = futures_to_store[future] 
            try:
                raw_responses = future.result()
            except Exception as e:
                logger.exception(f"An error ocurred scraping store {store}")
            else:
                load_raw_responses(raw_responses, session)
                
                normalized_responses = []
                for raw_response in raw_responses:
                    if raw_response["response_category"] != "product" or\
                            raw_response["response_type"] != "json":
                        continue
                    try:
                        normalized_responses.append(
                                {
                                    "raw_id": raw_response["raw_id"],
                                    "normalized_payload": scraper_by_store[store].parse(raw_response["payload"])
                                }
                        )
                    except Exception as e:
                        logger.exception(f"Failed parsing response: {raw_response}")
                load_normalized_responses(normalized_responses, session)

if __name__ == "__main__":
    main()
