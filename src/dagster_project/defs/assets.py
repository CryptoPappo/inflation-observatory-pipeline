import dagster as dg

from src.scapers.base import BaseScraper
from src.scrapers.scraper_coto import CotoSraper
from src.scrapers.scraper_carrefour import CarrefourScraper

def make_raw_asset(scraper_cls: BaseScraper) -> dg.Definitions:
    asset_key = f"scraper_{scraper_cls.store}"

    @dg.asset(name=asset_key)
    def _raw(context: dg.AssetExecutionContext):
        scraper = scraper_cls(scraper_id=context.run.run_id)
        raw_responses scraper.scrape()



    return 
