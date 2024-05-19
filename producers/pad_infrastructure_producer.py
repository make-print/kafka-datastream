from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "padInfrastructure"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "padInfrastructure"
            value = {
                "pneumaticPressurization": {
                    "pressure": round(random.uniform(90.0, 110.0), 2)
                },
                "hydraulicSystems": {
                    "pressure": round(random.uniform(140.0, 160.0), 2)
                },
                "battery": {
                    "wattage": round(random.uniform(4500.0, 5500.0), 2)
                },
                "fuelTank": {
                    "fullLevel": round(random.uniform(80.0, 100.0), 2),
                    "pressure": round(random.uniform(90.0, 110.0), 2),
                    "wattsLevel": round(random.uniform(2000.0, 3000.0), 2)
                },
                "pressureTank": {
                    "pressure": round(random.uniform(90.0, 110.0), 2)
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
