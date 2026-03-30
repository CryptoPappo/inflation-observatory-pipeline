from datetime import datetime
from sqlalchemy import (
        String,
        Text,
        DateTime,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
)

class Base(DeclarativeBase):
    pass

class RawResponses(Base):
    __tablenam__ = "raw_responses"

    scrape_id: Mapped[int] = mapped_column(primary_key=True)
    store: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(String(255))
    response_type: Mapped[str] = mapped_column(String(5))
    response_category: Mapped[str] = mapped_column(String(20))
    payload: Mapped[str] = mapped_column(Text)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    def __repr__(self) -> str:
        return f"RawResponses(scrape_id={self.scrape_id}, store={self.store},\
                response_category={self.response_category}, time={self.time})"

