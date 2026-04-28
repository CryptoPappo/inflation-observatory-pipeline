from collections.abc import Iterator
from sqlalchemy.dialects.postgresql import insert 
from sqlalchemy.orm.session import sessionmaker

from price_history.models.raw_tables import RawResponses, NormalizedResponses

def chunked(
        iterable: Iterator[dict],
        size: int
) -> Iterator[list[dict]]:
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []

    if batch:
        yield batch

def load_raw_responses(
        raw_iterable: Iterator[dict],
        Session: sessionmaker,
        batch_size: int = 100
) -> int:
    rows_count = 0
    for raw_responses in chunked(raw_iterable, batch_size):
        stmt = insert(RawResponses).values(raw_responses)
        stmt = stmt.on_conflict_do_nothing(
                index_elements=[RawResponses.raw_id]
        )
        with Session() as session:
            rows = session.execute(stmt)
            rows_count += rows.rowcount

            session.commit()
    
    return rows_count

def load_normalized_responses(
        normalized_responses: Iterator[dict],
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
