import dagster as dg
from sqlalchemy.orm import sessionmaker
from sqlalchemy import select
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
    ):
        scraper = scraper_cls(scrape_id=context.run.run_id)
        
        raw_iterator = scraper.scrape()

        rows_count = load_raw_responses(raw_iterator, postgres_session)
        context.log.info(f"{rows_count} Raw responses inserted for {scraper.store}")

    return dg.Definitions(
            assets=[_raw]
    )

def make_normalized_asset(scraper_cls: BaseScraper) -> dg.Definitions:
    asset_key = f"parser_{scraper_cls.store}"

    @dg.asset(name=asset_key)
    def _normalized(
            context: dg.AssetExecutionContext,
            postgres_session: dg.ResourceParam[sessionmaker]
    ):
        scraper = scraper_cls(scrape_id=context.run.run_id)

        rows_count = 0
        stmt = select(RawResponses)\
                .where(RawResponses.raw_id == context.run.run_id)\
                .where(RawResponses.response_category == "product")\
                .where(RawResponses.response_type == "json")\
                .execution_options(yield_per=100)
        with postgres_session() as session:
            for partition in session.scalars(stmt).partitions():
                normalized_iterator = scraper.parse(partition)

                rows_count += load_normalized_responses(normalized_iterator, session)
        context.log.info(f"{rows_count} Normalized responses insert for {scraper.store}")

    return dg.Definitions(
            assets=[_normalized]
    )

def make_warehouse_ready(scrapers_cls: list[BaseScraper]) -> dg.Definitions:
    deps = [f"parser_{scraper_cls.store}" for scraper_cls in scrapers_cls]

    @dg.asset(
            name="warehouse_ready",
            deps=deps
    )
    def _warehouse():
        pass

    return dg.Definitions(
            assets=[_warehouse]
    )

@dbt_assets(
        manifest=dbt_project.manifest_path,
        select="tag:daily"
)
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
    warehouse_assets = [make_warehouse_ready(scrapers_cls)]
    dbt_assets = [dg.Definitions(assets=[dbt_models])]

    total_assets = raw_assets + normalized_assets + warehouse_assets + dbt_assets

    return dg.Definitions.merge(*total_assets)   
