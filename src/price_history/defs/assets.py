import dagster as dg

from price_history.scrapers.base import BaseScraper
from price_history.scrapers.coto_scraper import CotoScraper
from price_history.scrapers.carrefour_scraper import CarrefourScraper
from price_history.loaders.load_raw_data import load_raw_responses, load_normalized_responses

def make_raw_asset(scraper_cls: BaseScraper) -> dg.Definitions:
    asset_key = f"scraper_{scraper_cls.store}"

    @dg.asset(name=asset_key)
    def _raw(context):
        scraper = scraper_cls(scrape_id=context.run.run_id)
        raw_responses = scraper.scrape()

        Session = context.resources.postgres_sessionmaker
        load_raw_responses(raw_responses, Session)
        
        return raw_responses

    return dg.Definitions(
            assets=[_raw]
    )

def make_normalized_asset(scraper_cls: BaseScraper) -> dg.Definitions:
    asset_key = f"parser_{scraper_cls.store}"

    @dg.asset(
            name=asset_key,
            ins={
                "raw_responses": dg.AssetIn(key=f"scraper_{scraper_cls.store}")
            }
    )
    def _normalized(context, raw_responses):
        scraper = scraper_cls(scrape_id=context.run.run_id)

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
                #logger.exception(f"Failed parsing response: {raw_response}")
                continue

        Session = context.resources.postgres_sessionmaker
        load_normalized_responses(normalized_responses, Session)

    return dg.Definitions(
            assets=[_normalized]
    )

@dg.definitions
def defs():
    scrapers_cls = [
            CotoScraper,
            CarrefourScraper
    ]
    raw_assets = [make_raw_asset(scraper_cls) for scraper_cls in scrapers_cls]
    normalized_assets = [make_normalized_asset(scraper_cls) for scraper_cls in scrapers_cls]
    total_assets = raw_assets + normalized_assets

    return dg.Definitions.merge(*total_assets)   
