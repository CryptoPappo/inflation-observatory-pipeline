from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from price_history.models.raw_tables import RawResponses, NormalizedResponses

def load_raw_responses(
        raw_responses: list[dict],
        Session: sessionmaker
) -> int:
    stmt = insert(RawResponses).values(raw_responses)
    stmt = stmt.on_conflict_do_nothing(
            index_elements=[RawResponses.raw_id]
    )
    with Session() as session:
        rows = session.execute(stmt)
        rows_count = rows.rowcount

        session.commit()
    
    return rows_count

def load_normalized_responses(
        normalized_responses: list[dict],
        Session: sessionmaker
) -> int:
    stmt = insert(NormalizedResponses).values(normalized_responses)
    stmt = stmt.on_conflict_do_nothing(
            index_elements=[NormalizedResponses.raw_id]
    )
    with Session() as session:
        rows = session.execute(stmt)
        rows_count = rows.rowcount
        
        session.commit()

    return rows_count
