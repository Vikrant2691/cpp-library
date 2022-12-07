import pandas as pd
import numpy as np
from library import recommendations
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

application = app = __name__
DATABASE_URI = 'postgresql+psycopg2://postgres:12345678@database-1.cuxbcbbcz4bk.us-east-1.rds.amazonaws.com/postgres'

engine = create_engine(DATABASE_URI)#.raw_connection()

Session = sessionmaker(bind=engine)
Base = declarative_base()

class User(Base):
    __tablename__ = "user"
    id = Column(Integer, primary_key=True)
    username = Column(String(20), nullable=False, unique=True)
    password = Column(String(80), nullable=False)
    email = Column(String(20), nullable=False, unique=True)
    mobile = Column(String(20), nullable=False, unique=True)
    recommendation = Column(String(20), nullable=True)

    def __repr__(self):
        return f'<Task : {self.id}>'


class Orders(Base):
    
    __tablename__ = "orders"
    id = Column(Integer, primary_key=True)
    bookId = Column(Integer, nullable=False)
    orderDate = Column(DateTime, default=datetime.utcnow)
    userId = Column(Integer, nullable=False)
    review = Column(Integer, nullable=True)

    def __repr__(self):
        return f'<Task : {self.id}>'

@contextmanager
def session_scope():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def updateRecommendations(userId):
    
    
    with session_scope() as s:
        df = pd.read_sql(
        "select \"userId\",\"bookId\",review from orders", engine).fillna(0)
        df = df.astype('int64')
        print(df.info())
        ratings_matrix = df.pivot(
            index='userId', columns='bookId', values='review')
        ratings_matrix.fillna(0, inplace=True)
        ratings_matrix = ratings_matrix.astype(np.int32)
        ratings_matrix.head()
        user_recommendations = recommendations(userId, ratings_matrix)
        print(user_recommendations)
        s.query(User).filter(User.id == userId).update(
            {'recommendation': user_recommendations})
    

def getApp():
    return app

if __name__ == '__main__':

    updateRecommendations(6)
