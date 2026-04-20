import os
from datetime import datetime
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from prefect import flow, task, get_run_logger

from src.models.raw_tables import Base
from src.scrapers.coto_scraper import CotoScraper
from src.scrapers.carrefour_scraper import CarrefourScraper
from src.loaders.load_raw_data import load_raw_responses, load_normalized_responses

@task(
    retries=3,
    retry_delay_seconds=60 * 15
)
def scrape(scraper):
    logger = get_run_logger()

    logger.info(f"Starting scrape for {scraper.store}")

    try:
        raw_responses = scraper.scrape()
        logger.info(f"Scraped {len(raw_responses)} items for {scraper.store}")        
        return raw_responses
    except Exception as e:
        logger.exception(f"Scrape failed for {scraper.store}")

@task(
    retries=2,
    retry_delay_seconds=120
)
def load_raw(
        raw_responses,
        session
):
    load_raw_responses(raw_responses, session)

@task
def parse(
        scraper,
        raw_responses
):
    logger = get_run_logger()
    
    logger.info(f"Starting parsing for {scraper.store}")

    normalized_responses = []
    for raw_response in raw_responses:
        if raw_response["response_category"] != "product" or\
                raw_response["response_type"] != "json":
            continue
        try:
            normalized_responses.append(
                    {
                        "raw_id": raw_response["raw_id"],
                        "normalized_payload": scraper.parse(raw_response["payload"])
                    }
            )
        except Exception as e:
            logger.exception(f"Failed parsing {scraper.store} response: {raw_response}")

    return normalized_responses

@task(
    retries=2,
    retry_delay_seconds=120
)
def load_normalized(
        normalized_responses,
        session
):
    load_normalized_responses(normalized_responses, session)

@flow
def main():
    logger = get_run_logger()

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

    scrapers_cls = [
            CotoScraper,
            CarrefourScraper
    ]
    scrape_id = datetime.utcnow().strftime("%Y-%m-%d")
    for scraper_cls in scrapers_cls:
        scraper = scraper_cls(scrape_id=scrape_id)

        raw = scrape.submit(scraper)
        load_raw.submit(raw, session, wait_for=[raw])

        parsed = parse.submit(scraper, raw, wait_for=[raw])
        load_normalized.submit(parsed, session, wait_for=[parsed])
    
if __name__ == "__main__":
    main()
