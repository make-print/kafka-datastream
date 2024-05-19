from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "payload"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "payload"
            value = {
                "id": "PL789",
                "type": "Satellite",
                "status": {
                    "health": random.choice(["Operational", "Non-Operational"]),
                    "position": {
                        "latitude": 28.5721,
                        "longitude": -80.648,
                        "altitude": round(random.uniform(500.0, 600.0), 2)
                    },
                    "telemetry": {
                        "rfFrequencies": [round(random.uniform(2.0, 6.0), 2) for _ in range(2)],
                        "weight": round(random.uniform(200.0, 300.0), 2)
                    }
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
