import os
import dagster as dg
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dagster_dbt import DbtCliResource, DbtProject

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

dbt_project_directory = Path(__file__).absolute().parent.parent.parent.parent / "supermarket_prices"
dbt_project = DbtProject(project_dir=dbt_project_directory)

dbt_resource = DbtCliResource(project_dir=dbt_project)

@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "dbt": dbt_resource,
            "postgres_session": sessionmaker(bind=engine)
        }
    )
