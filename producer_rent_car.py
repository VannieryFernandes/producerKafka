from kafka import KafkaProducer
import json
from datetime import datetime
import time
import uuid
import random
def new_vehicle_message(i):
    now = datetime.now()


    vehicle = {
                      "id": str(uuid.uuid4()),
                      "model":  "Ford f250",
                      "type_car": "SUV",
                      "licence_number": f"tam12{i}",
                      "mileage": "15000",
                      "reserve_vehicle": False,
                      "date_status": str(now)
                    }
                  
   

    return vehicle


def kafka_connection():
    producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092',
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer


def kafka_producer_message():

    producer = kafka_connection()

    for i in range(0, 10):
        time.sleep(0.5)
        print(i)
        vehicle_message = new_vehicle_message(i)
        producer.send('kfk.vehicle', vehicle_message)


kafka_producer_message()