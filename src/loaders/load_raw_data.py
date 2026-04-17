from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from src.utils.logging import get_logger
from src.models.raw_tables import RawResponses, NormalizedResponses
logger = get_logger("load_raw_data")

def load_raw_responses(
        raw_responses: list[dict],
        Session: sessionmaker
):
    stmt = insert(RawResponses).values(raw_responses)
    stmt = stmt.on_conflict_do_nothing(
            index_elements=[RawResponses.raw_id]
    )
    with Session() as session:
        rows = session.execute(stmt)
        rows_count = rows.rowcount

        session.commit()

    logger.info(f"Raw responses insert: attempted={len(raw_responses)} inserted={rows_count}")

def load_normalized_responses(
        normalized_responses: list[dict],
        Session: sessionmaker
):
    stmt = insert(NormalizedResponses).values(normalized_responses)
    stmt = stmt.on_conflict_do_nothing(
            index_elements=[NormalizedResponses.raw_id]
    )
    with Session() as session:
        rows = session.execute(stmt)
        rows_count = rows.rowcount
        
        session.commit()

    logger.info(f"Normalized responses insert: attempted={len(normalized_responses)} inserted={rows_count}")
