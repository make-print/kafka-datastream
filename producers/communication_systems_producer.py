from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config

# Initialize the state
state = {
    "emergencyComms": {
        "status": "Active",
        "reroutePlan": {
            "primary": "Satellite",
            "backup": "Ground"
        }
    },
    "astronautComms": {
        "status": "Active",
        "reroutePlan": {
            "primary": "Satellite",
            "backup": "Ground"
        }
    },
    "telemetryComms": {
        "status": "Active",
        "reroutePlan": {
            "primary": "Satellite",
            "backup": "Ground"
        }
    }
}


def update_status(status):
    # Occasionally change the status
    if random.random() < 0.1:  # 10% chance to change the status
        return "Inactive" if status == "Active" else "Active"
    return status


def update_state(state):
    state["emergencyComms"]["status"] = update_status(state["emergencyComms"]["status"])
    state["astronautComms"]["status"] = update_status(state["astronautComms"]["status"])
    state["telemetryComms"]["status"] = update_status(state["telemetryComms"]["status"])
    return state


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
