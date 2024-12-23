#!/usr/bin/env python3
from confluent_kafka import Consumer, KafkaException

# Kafka configuration
kafka_config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest',  # Start reading from the beginning if no offset is committed
}

default_topic = 'my-topic'


# Create Kafka consumer
consumer = Consumer(kafka_config)

def consume_one_message(topic):
    try:
        consumer.subscribe([topic])

        print(f"Listening for messages on topic '{topic}'...")
        # Poll for a message (timeout in seconds)
        msg = consumer.poll(timeout=10.0)

        if msg is None:
            print("No message received within the timeout period.")
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            print(f"Received message: {msg.value().decode('utf-8')} from topic '{msg.topic()}'")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    kafka_topic = default_topic
    consume_one_message(kafka_topic)
