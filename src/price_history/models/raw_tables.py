from datetime import datetime
from sqlalchemy import (
        ForeignKey,
        String,
        Text,
        DateTime,
        JSON
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship
)

class Base(DeclarativeBase):
    pass

class RawResponses(Base):
    __tablename__ = "raw_responses"
    
    raw_id: Mapped[str] = mapped_column(
            String(32),
            primary_key=True
    )
    scrape_id: Mapped[str] = mapped_column(String(255))
    store: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(String(255))
    response_type: Mapped[str] = mapped_column(String(5))
    response_category: Mapped[str] = mapped_column(String(20))
    payload: Mapped[str] = mapped_column(Text)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    normalized: Mapped["NormalizedResponses"] = relationship(back_populates="raw")

    def __repr__(self) -> str:
        return f"RawResponses(scrape_id={self.scrape_id}, store={self.store},\
                response_category={self.response_category}, time={self.time})"

class NormalizedResponses(Base):
    __tablename__ = "normalized_responses"

    raw_id: Mapped[str] = mapped_column(
            String(32),
            ForeignKey("raw_responses.raw_id"),
            primary_key=True
    )
    normalized_payload: Mapped[JSON] = mapped_column(JSON)

    raw: Mapped[RawResponses] = relationship(back_populates="normalized")

    def __repr__(self) -> str:
        return f"NormalizedRespones(raw_id={self.raw_id})"
