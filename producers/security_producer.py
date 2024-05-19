from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "security"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "security"
            value = {
                "protocol": random.choice(["AES", "RSA", "3DES"]),
                "encryptionLevel": random.choice(["128-bit", "256-bit"]),
                "accessControl": {
                    "nasa": random.choice([True, False]),
                    "faa": random.choice([True, False]),
                    "safetyAgency": random.choice([True, False]),
                    "aerospaceCompany": random.choice([True, False])
                }
            }
            producer.produce(topic, key=key, value=json.dumps(value))
            print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")

            producer.poll(0)
            time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
