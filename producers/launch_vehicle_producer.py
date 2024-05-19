from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "launchVehicle"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "launchVehicle"
            value = {
                "id": "LV456",
                "type": "Rocket",
                "status": {
                    "health": random.choice(["Good", "Fair", "Poor"]),
                    "position": {
                        "latitude": 28.5721,
                        "longitude": -80.648,
                        "altitude": round(random.uniform(0.0, 100.0), 2)
                    },
                    "telemetry": {
                        "pressure": round(random.uniform(900.0, 1100.0), 2),
                        "voltage": round(random.uniform(20.0, 30.0), 2),
                        "heartbeat": random.choice([True, False]),
                        "fuelConsumption": round(random.uniform(0.0, 10.0), 2),
                        "estimatedTimeToFull": round(random.uniform(0.0, 3600.0), 2)
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
