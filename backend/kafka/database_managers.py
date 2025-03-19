from typing import Optional
from sqlalchemy import DateTime, Column, func, create_engine, and_, select
from sqlalchemy.orm import Mapped, mapped_column, Session
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime


Base = declarative_base()
db_location = "sqlite:///big_brother.db"


class Tweet(Base):
    __tablename__ = "processed_tweets"

    id: Mapped[int] = mapped_column(primary_key=True)
    time: Mapped[DateTime] = Column(DateTime(timezone=True), default=func.now())
    tweet: Mapped[str]
    ner: Mapped[str]
    category: Mapped[Optional[str]]
    sentiment: Mapped[Optional[str]]

    def __repr__(self) -> str:
        return f"Tweet (id={self.id!r}, time={self.time!r}, text={self.tweet!r})"


class DatabaseWriter:
    def __init__(self):
        self.engine = create_engine(db_location, echo=True)
        self.create_tables()

    def create_tables(self):
        Base.metadata.create_all(self.engine)

    def add_rows(self, rows: list):
        with Session(self.engine) as session:
            tweets = [Tweet(**row) for row in rows]
            session.add_all(tweets)
            session.commit()


class DatabaseReader:
    def __init__(self):
        self.engine = create_engine(db_location, echo=True)
        self.session = Session(self.engine)

    def read_interval(self, start: datetime, end: datetime = datetime.now()):
        rows = self.session.query(Tweet).filter(and_(Tweet.time >= start, Tweet.time <= end))
        return rows

    def get_count(self):
        return self.session.query(Tweet).count()

    def get_start(self):
        return self.session.query(Tweet.time, func.min(Tweet.time))

    def get_all(self):
        return self.session.execute(select(Tweet)).all()

    def close(self):
        self.session.close()
