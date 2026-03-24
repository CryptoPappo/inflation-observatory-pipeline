from datetime import datetime
from sqlalchemy import (
        BigInteger,
        String,
        Text,
        DateTime,
        JSON,
        ForeignKey
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship
)

class Base(DeclarativeBase):
    pass

class RawPages(Base):
    __tablename__ = "raw_pages"

    scrape_id: Mapped[int] = mapped_column(
            BigInteger,
            primary_key=True
    )
    store: Mapped[str] = mapped_column(String(255))
    url: Mapped[str] = mapped_column(String(255))
    html: Mapped[str] = mapped_column(Text)
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    products: Mapped["RawProducts"] = relationship(back_populates="raw_pages")

    def __repr__(self) -> str:
        return f"RawPages(scrape_id={self.scrape_id}, url={self.url})"

class RawProducts(Base):
    __tablename__ = "raw_products"

    scrape_id: Mapped[int] = mapped_column(
            BigInteger,
            ForeignKey("raw_pages.scrape_id"),
            primary_key=True
    )
    payload: Mapped[dict] = mapped_column(JSON)

    pages: Mapped[RawPages] = relationship(back_populates="raw_products")

    def __repr__(self) -> str:
        return f"RawProducts(scrape_id={self.scrape_id})"
