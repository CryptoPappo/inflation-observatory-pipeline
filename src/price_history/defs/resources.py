import os
import dagster as dg
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"))

@dg.definitions
def resources():    
    return dg.Definitions(
            resources={
                "postgres_session": sessionmaker(bind=engine)
            },
    )
