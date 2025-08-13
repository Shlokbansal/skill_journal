from time import sleep
from json import dumps
from kafka import KafkaProducer
import random

# Simulate Kafka Producer for heart rate data
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

while True:
    data = {
        'patient_id': f'p{random.randint(1,5)}',
        'heart_rate': random.randint(60, 100)
    }
    producer.send('heart_rate_topic', value=data)
    print(f"Sent: {data}")
    sleep(2)
