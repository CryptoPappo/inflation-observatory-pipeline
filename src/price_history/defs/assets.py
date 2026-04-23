import dagster as dg

from price_history.scrapers.base import BaseScraper
from price_history.scrapers.coto_scraper import CotoScraper
from price_history.scrapers.carrefour_scraper import CarrefourScraper
from price_history.loaders.load_raw_data import load_raw_responses, load_normalized_responses

def make_raw_asset(scraper_cls: BaseScraper):
    asset_key = f"scraper_{scraper_cls.store}"

    @dg.asset(name=asset_key)
    def _raw(context: dg.AssetExecutionContext):
        scraper = scraper_cls(scraper_id=context.run.run_id)
        raw_responses = scraper.scrape()

        Session = context.postgres_sessionmaker
        load_raw_responses(raw_responses, Session)
        
        return raw_responses

    return _raw


