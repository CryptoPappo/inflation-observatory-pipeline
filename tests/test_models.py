from sqlalchemy import create_engine, inspect

from price_history.models.raw_tables import Base

def test_tables_created():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)

    inspector = inspect(engine)
    tables = inspector.get_table_names()

    assert len(tables) == 2
    assert "raw_responses" in tables
    assert "normalized_responses" in tables
