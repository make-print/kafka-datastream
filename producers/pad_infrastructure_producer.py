from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config

# Initialize the state
state = {
    "pneumaticPressurization": {
        "pressure": 100.0
    },
    "hydraulicSystems": {
        "pressure": 150.0
    },
    "battery": {
        "wattage": 5000.0
    },
    "fuelTank": {
        "fullLevel": 90.0,
        "pressure": 100.0,
        "wattsLevel": 2500.0
    },
    "pressureTank": {
        "pressure": 100.0
    }
}


def update_state(state):
    # Update pneumaticPressurization pressure
    state["pneumaticPressurization"]["pressure"] += random.uniform(-0.5, 0.5)
    state["pneumaticPressurization"]["pressure"] = max(90.0, min(110.0, state["pneumaticPressurization"]["pressure"]))

    # Update hydraulicSystems pressure
    state["hydraulicSystems"]["pressure"] += random.uniform(-0.5, 0.5)
    state["hydraulicSystems"]["pressure"] = max(140.0, min(160.0, state["hydraulicSystems"]["pressure"]))

    # Update battery wattage
    state["battery"]["wattage"] += random.uniform(-50.0, 50.0)
    state["battery"]["wattage"] = max(4500.0, min(5500.0, state["battery"]["wattage"]))

    # Update fuelTank levels and pressure
    state["fuelTank"]["fullLevel"] += random.uniform(-0.2, 0.2)
    state["fuelTank"]["fullLevel"] = max(80.0, min(100.0, state["fuelTank"]["fullLevel"]))
    state["fuelTank"]["pressure"] += random.uniform(-0.5, 0.5)
    state["fuelTank"]["pressure"] = max(90.0, min(110.0, state["fuelTank"]["pressure"]))
    state["fuelTank"]["wattsLevel"] += random.uniform(-50.0, 50.0)
    state["fuelTank"]["wattsLevel"] = max(2000.0, min(3000.0, state["fuelTank"]["wattsLevel"]))

    # Update pressureTank pressure
    state["pressureTank"]["pressure"] += random.uniform(-0.5, 0.5)
    state["pressureTank"]["pressure"] = max(90.0, min(110.0, state["pressureTank"]["pressure"]))

    return state


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
