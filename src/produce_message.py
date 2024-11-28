#!/usr/bin/python3
import json
from confluent_kafka import Producer

# Configuration for Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust if needed
}

# Create a Kafka producer
producer = Producer(kafka_config)

def delivery_report(err, msg):
    """Callback function to handle delivery reports."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_json_to_kafka(file_path, topic):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            message = json.dumps(data)

            producer.produce(topic, message, callback=delivery_report)
            producer.flush()

            print("Message sent successfully!")
    # except Exception as e:
    except FileNotFoundError:
        print("Error: Cannot find the sample json data.")
        print("Please run this from the main project library")
        print("Usage: src/produce_message.py")
        print("Or")
        print("Usage: python3 src/produce_message.py")
        # print(f"Error: {e}")

if __name__ == "__main__":
    json_file_path = 'sample_data/sample_data_01.json'  # Path to your JSON file
    kafka_topic = 'topic-try01'

    send_json_to_kafka(json_file_path, kafka_topic)
        
