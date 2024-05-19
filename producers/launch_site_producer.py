from confluent_kafka import Producer
import time
import signal
import json
import random
from datetime import datetime
from utils import read_config

# Initialize the state
state = {
    "id": "LS123",
    "name": "Cape Canaveral",
    "location": {
        "latitude": 28.5721,
        "longitude": -80.648,
        "altitude": 3.0
    },
    "powerSource": {
        "type": "Solar",
        "wattage": 3000.0,
        "in_volt": 220.0,
        "out_volt": 115.0,
        "current": 30.0,
        "frequency": 55.0
    }
}


def update_state(state):
    # Update powerSource data
    state["powerSource"]["wattage"] += random.uniform(-50.0, 50.0)
    state["powerSource"]["wattage"] = max(1000.0, min(5000.0, state["powerSource"]["wattage"]))

    state["powerSource"]["in_volt"] += random.uniform(-1.0, 1.0)
    state["powerSource"]["in_volt"] = max(200.0, min(240.0, state["powerSource"]["in_volt"]))

    state["powerSource"]["out_volt"] += random.uniform(-0.5, 0.5)
    state["powerSource"]["out_volt"] = max(110.0, min(120.0, state["powerSource"]["out_volt"]))

    state["powerSource"]["current"] += random.uniform(-1.0, 1.0)
    state["powerSource"]["current"] = max(10.0, min(50.0, state["powerSource"]["current"]))

    state["powerSource"]["frequency"] += random.uniform(-0.5, 0.5)
    state["powerSource"]["frequency"] = max(50.0, min(60.0, state["powerSource"]["frequency"]))

    return state


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
            global state
            state = update_state(state)
            value = state
            producer.produce(topic, key=key, value=json.dumps(value))
            print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")

            producer.poll(0)
            time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
