#!/usr/bin/env python3

import json
import argparse
from confluent_kafka import Producer

# Configuration for Kafka
kafka_config = {
    'bootstrap.servers': 'localhost:9092',  # Adjust if needed
}

default_topic = 'my-topic'

# Create a Kafka producer
producer = Producer(kafka_config)

def delivery_report(err, msg):
    """Callback function to handle delivery reports."""
    if err:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def send_json_to_kafka(file_path, topic, num_messages=1):
    try:
        with open(file_path, 'r') as file:
            data = json.load(file)
            message = json.dumps(data)

            for _ in range(num_messages):
                producer.produce(topic, message, callback=delivery_report)
                producer.flush()

            print(f"{num_messages} message(s) sent successfully!")
    except FileNotFoundError:
        print("Error: Cannot find the sample json data.")
        print("Please run this from the main project library")
        print("Usage: src/produce_message.py")
        print("Or")
        print("Usage: python3 src/produce_message.py")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka producer script")
    parser.add_argument("-n", "--num_messages", type=int, default=1,
                        help="Number of messages to send (default: 1)")
    args = parser.parse_args()

    json_file_path = 'sample_data/sample_data_01.json'  # Path to your JSON file
    kafka_topic = default_topic

    send_json_to_kafka(json_file_path, kafka_topic, args.num_messages)