import os
import dagster as dg
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

@dg.definitions
def resources():
    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"))

    return dg.Definitions(
            resources={
                "postgres_sessionmaker": sessionmaker(bind=engine)
            },
    )
