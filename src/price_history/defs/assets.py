import dagster as dg
from sqlalchemy.orm import sessionmaker
from dagster_dbt import DbtCliResource, dbt_assets, build_dbt_asset_selection

from price_history.scrapers.base import BaseScraper
from price_history.scrapers.coto_scraper import CotoScraper
from price_history.scrapers.carrefour_scraper import CarrefourScraper
from price_history.loaders.load_raw_data import load_raw_responses, load_normalized_responses
from .resources import dbt_project

def make_raw_asset(scraper_cls: BaseScraper) -> dg.Definitions:
    asset_key = f"scraper_{scraper_cls.store}"

    @dg.asset(name=asset_key)
    def _raw(
            context: dg.AssetExecutionContext,
            postgres_session: dg.ResourceParam[sessionmaker]
    ) -> list[dict]:
        scraper = scraper_cls(scrape_id=context.run.run_id)
        raw_responses = scraper.scrape()

        load_raw_responses(raw_responses, postgres_session)
        
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
    def _normalized(
            context: dg.AssetExecutionContext,
            postgres_session: dg.ResourceParam[sessionmaker],
            raw_responses: list[dict]
    ):
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

        load_normalized_responses(normalized_responses, postgres_session)

    return dg.Definitions(
            assets=[_normalized]
    )

@dbt_assets(manifest=dbt_project.manifest_path, select="tag:daily")
def dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()


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
