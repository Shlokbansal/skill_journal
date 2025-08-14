"""
Kafka Streaming Simulation
Simulates patient heart rate data for real-time processing.
"""

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
import json
import time
import random
import sys

def create_kafka_producer():
    """Create Kafka producer with error handling"""
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',  # Change if using a cloud Kafka
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=3,
            retry_backoff_ms=1000
        )
        # Test connection
        producer.bootstrap_connected()
        return producer
    except NoBrokersAvailable:
        print("Error: No Kafka brokers available at localhost:9092")
        print("Please ensure Kafka is running:")
        print("1. Start Zookeeper: bin/zookeeper-server-start.sh config/zookeeper.properties")
        print("2. Start Kafka: bin/kafka-server-start.sh config/server.properties")
        print("3. Or use Docker: docker run -p 9092:9092 apache/kafka:2.8.1")
        return None
    except Exception as e:
        print(f"Error connecting to Kafka: {e}")
        return None

# 1. Configure Kafka producer
producer = create_kafka_producer()
if not producer:
    sys.exit(1)

# 2. Simulate health data stream
try:
    while True:
        patient_data = {
            "patient_id": random.randint(1000, 1010),
            "heart_rate": random.randint(60, 100),  # bpm
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        try:
            future = producer.send('patient_vitals', value=patient_data)
            future.get(timeout=10)  # Wait for send confirmation
            print(f"Sent: {patient_data}")
        except Exception as e:
            print(f"Failed to send message: {e}")
        
        time.sleep(2)  # send every 2 seconds
except KeyboardInterrupt:
    print("\nShutting down producer...")
    producer.close()
    sys.exit(0)
