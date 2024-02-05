import requests
from time import sleep
import random
from multiprocessing import Process
import boto3
import json
import sqlalchemy
from sqlalchemy import text
from datetime import datetime


random.seed(100)

class DateTimeEncoder(json.JSONEncoder):
    """ Custom encoder for datetime objects """
    def default(self, obj):
        if isinstance(obj, datetime):
            # Format datetime objects as strings
            return obj.isoformat()
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


class AWSDBConnector:

    def __init__(self):

        self.HOST = "pinterestdbreadonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com"
        self.USER = 'project_user'
        self.PASSWORD = ':t%;yCY3Yjg'
        self.DATABASE = 'pinterest_data'
        self.PORT = 3306
        
    def create_db_connector(self):
        engine = sqlalchemy.create_engine(f"mysql+pymysql://{self.USER}:{self.PASSWORD}@{self.HOST}:{self.PORT}/{self.DATABASE}?charset=utf8mb4")
        return engine


new_connector = AWSDBConnector()

def send_to_api_gateway(data, topic_name):
    invoke_url = f"https://rfrcndbjtb.execute-api.us-east-1.amazonaws.com/prod/{topic_name}"
    headers = {'Content-Type': 'application/vnd.kafka.json.v2+json'}
    # Prepare payload
    payload = json.dumps({"records": [{"value": data}]}, cls=DateTimeEncoder)
    # Send POST request
    response = requests.post(invoke_url, headers=headers, data=payload)
    return response


def run_infinite_post_data_loop():
    while True:
        sleep(random.randrange(0, 2))
        random_row = random.randint(0, 11000)
        engine = new_connector.create_db_connector()

        with engine.connect() as connection:
            # Fetch and send data from pinterest_data table
            pin_string = text(f"SELECT * FROM pinterest_data LIMIT {random_row}, 1")
            pin_selected_row = connection.execute(pin_string)
            
            for row in pin_selected_row:
                pin_result = dict(row._mapping)
            
            response = send_to_api_gateway(pin_result, "0a1667ad2f7f.pin")
            print(f"Pin Response: {response.status_code}, {response.text}")

            # Fetch and send data from geolocation_data table
            geo_string = text(f"SELECT * FROM geolocation_data LIMIT {random_row}, 1")
            geo_selected_row = connection.execute(geo_string)
            
            for row in geo_selected_row:
                geo_result = dict(row._mapping)

            response = send_to_api_gateway(geo_result, "0a1667ad2f7f.geo")
            print(f"Geo Response: {response.status_code}, {response.text}")

            # Fetch and send data from user_data table
            user_string = text(f"SELECT * FROM user_data LIMIT {random_row}, 1")
            user_selected_row = connection.execute(user_string)
            
            for row in user_selected_row:
                user_result = dict(row._mapping)
            
            response = send_to_api_gateway(user_result, "0a1667ad2f7f.user")
            print(f"User Response: {response.status_code}, {response.text}")



if __name__ == "__main__":
    run_infinite_post_data_loop()