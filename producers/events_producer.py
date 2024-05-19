from confluent_kafka import Producer
import time
import signal
import json
import random
from datetime import datetime
from utils import read_config


def run_producer():
    config = read_config()
    topic = "events"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "events"
            value = [
                {
                    "timestamp": datetime.utcnow().isoformat() + "Z",
                    "eventType": random.choice(["System Check", "Launch", "Abort"]),
                    "details": "Event details go here."
                }
            ]
            producer.produce(topic, key=key, value=json.dumps(value))
            print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")

            producer.poll(0)
            time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
