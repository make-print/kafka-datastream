from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "communicationSystems"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "communicationSystems"
            value = {
                "emergencyComms": {
                    "status": random.choice(["Active", "Inactive"]),
                    "reroutePlan": {
                        "primary": "Satellite",
                        "backup": "Ground"
                    }
                },
                "astronautComms": {
                    "status": random.choice(["Active", "Inactive"]),
                    "reroutePlan": {
                        "primary": "Satellite",
                        "backup": "Ground"
                    }
                },
                "telemetryComms": {
                    "status": random.choice(["Active", "Inactive"]),
                    "reroutePlan": {
                        "primary": "Satellite",
                        "backup": "Ground"
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
