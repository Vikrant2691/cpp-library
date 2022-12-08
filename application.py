import pandas as pd
import numpy as np
# from library import recommendations
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
import logging
import boto3
from botocore.exceptions import ClientError
import time
import json
from cpp_library_package.library import recommendations


application = app = __name__
DATABASE_URI = 'postgresql+psycopg2://postgres:12345678@database-1.cuxbcbbcz4bk.us-east-1.rds.amazonaws.com/postgres'

engine = create_engine(DATABASE_URI)

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
    

def consume_message(queue_name):
        
    try:
        # Create a session and use it to make our client
        session = boto3.session.Session()
        sqs_client = session.client('sqs')
            
            
            
        # retrive the URL of an existing Amazon SQS queue
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
            
            
        while True:
            print('\n\t\t<=== requesting messages from the queue...\n')
            # receive a message from the specified queue
            response = sqs_client.receive_message(QueueUrl=queue_url,
                    MaxNumberOfMessages=1, # a number between 1 and 10
                                VisibilityTimeout=10, #default 30 seconds
            )
                
            messages = response.get('Messages')
            if messages != None:
                
                messages = response['Messages'] # a list with all the messages
                
                # in this example we only retrieved one message, so the list contains only one element
                current_message = messages[0] # retrieve the message from the list
                print("\n<=== The message I'm proccessing is:\n {}".format(current_message['Body']))
                print(type(current_message['Body']))
                result=json.loads(current_message['Body'])
                userid=result['userid']
                updateRecommendations(userid)
                receipt_handle = current_message['ReceiptHandle']
                response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
                print("\n<=== sqs_client deleted the message, and the response is {}".format(response))
            else:
                print('No message has been received, will go to sleep...')
                time.sleep(10)
                    
                
            
    except ClientError as e:
        logging.error(e)
        return False
        
        
def consume_message_short(queue_name):
        
    try:
        
        print("----------------------------------Hi")
        # Create a session and use it to make our client
        session = boto3.session.Session()
        sqs_client = session.client('sqs', region_name='us-east-1')
            
            
            
        # retrive the URL of an existing Amazon SQS queue
        response = sqs_client.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
            
            
        print('\n\t\t<=== requesting messages from the queue...\n')
        # receive a message from the specified queue
        response = sqs_client.receive_message(QueueUrl=queue_url,
            MaxNumberOfMessages=1, # a number between 1 and 10
            VisibilityTimeout=10, #default 30 seconds
        )
                
        messages = response.get('Messages')
        if messages != None:
                
            messages = response['Messages'] # a list with all the messages
                
            # in this example we only retrieved one message, so the list contains only one element
            current_message = messages[0] # retrieve the message from the list
            print("\n<=== The message I'm proccessing is:\n {}".format(current_message['Body']))
            print(type(current_message['Body']))
            result=json.loads(current_message['Body'])
            userid=result['userid']
            updateRecommendations(userid)
            receipt_handle = current_message['ReceiptHandle']
            response = sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
            print("\n<=== sqs_client deleted the message, and the response is {}".format(response))
        else:
            print('No message has been received, will go to sleep...')
                    
                
            
    except ClientError as e:
        logging.error(e)
        return False
    
    return True
        
        

if __name__ == '__main__':
    consume_message("book-orders-q")
    