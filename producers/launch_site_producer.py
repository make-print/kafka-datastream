from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "launchSite"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "launchSite"
            value = {
                "id": "LS123",
                "name": "Cape Canaveral",
                "location": {
                    "latitude": 28.5721,
                    "longitude": -80.648,
                    "altitude": 3.0
                },
                "powerSource": {
                    "type": "Solar",
                    "wattage": round(random.uniform(1000.0, 5000.0), 2),
                    "in_volt": round(random.uniform(200.0, 240.0), 2),
                    "out_volt": round(random.uniform(110.0, 120.0), 2),
                    "current": round(random.uniform(10.0, 50.0), 2),
                    "frequency": round(random.uniform(50.0, 60.0), 2)
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
