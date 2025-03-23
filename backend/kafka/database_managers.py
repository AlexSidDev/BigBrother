from typing import Optional
from sqlalchemy import DateTime, Column, func, create_engine, and_, select
from sqlalchemy.orm import Mapped, mapped_column, Session
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import func
from datetime import datetime
import pandas as pd

import logging

logging.getLogger('sqlalchemy.engine.Engine').disabled = True

Base = declarative_base()
db_location = "sqlite:///big_brother_new.db"


class Tweet(Base):
    __tablename__ = "tweets"

    id: Mapped[int] = mapped_column(primary_key=True)
    time: Mapped[DateTime] = Column(
        DateTime(timezone=True), default=func.now())
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
        rows = self.session.query(Tweet).filter(
            Tweet.time.between(start, end)).order_by(Tweet.time).all()
        return self.rows2df(rows)

    def get_count(self):
        return self.session.query(Tweet).count()

    def get_start(self):
        # return self.session.query(Tweet.time, func.min(Tweet.time))
        min_time = self.session.query(func.min(Tweet.time)).scalar()
        rows = self.session.query(Tweet).filter(
            Tweet.time == min_time)
        return self.rows2df(rows).iloc[0]
    
    def get_last(self):
        # return self.session.query(Tweet.time, func.min(Tweet.time))
        max_time = self.session.query(func.max(Tweet.time)).scalar()
        rows = self.session.query(Tweet).filter(
            Tweet.time == max_time)
        return self.rows2df(rows).iloc[0]

    def get_all(self):
        rows = self.session.query(Tweet).all()
        return self.rows2df(rows)

    def close(self):
        self.session.close()

    def rows2df(self, rows):
        data = [row.__dict__ for row in rows]

        df = pd.DataFrame(data)
        df = df.drop("_sa_instance_state", axis=1, errors="ignore")

        return df
