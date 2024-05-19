from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config


def run_producer():
    config = read_config()
    topic = "weatherData"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "weatherData"
            value = {
                "temperature": round(random.uniform(20.0, 30.0), 2),
                "humidity": round(random.uniform(50.0, 100.0), 2),
                "windSpeed": round(random.uniform(0.0, 20.0), 2),
                "windDirection": round(random.uniform(0.0, 360.0), 2),
                "lightningDetected": random.choice([True, False])
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
