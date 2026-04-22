import os
import dagster as dg
from sqlalchemy.orm import sessionmaker

@dg.resource
def postgres_sessionmaker():
    engine = create_engine(os.getenv("DATABASE_URL"))
    Session = sessionmaker(bind=engine)
    yield Session
    engine.dispose()

