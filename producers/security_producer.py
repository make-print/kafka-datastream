from confluent_kafka import Producer
import time
import signal
import json
import random
from datetime import datetime, timedelta
from utils import read_config

# Initialize state
state = {
    "protocol": "AES",
    "encryptionLevel": "256-bit",
    "accessControl": {
        "nasa": True,
        "faa": False,
        "safetyAgency": True,
        "aerospaceCompany": False
    },
    "lastProtocolChange": datetime.utcnow()
}

def update_state(state):
    # Periodically change the protocol every hour
    if datetime.utcnow() - state["lastProtocolChange"] > timedelta(hours=1):
        state["protocol"] = random.choice(["AES", "RSA", "3DES"])
        state["lastProtocolChange"] = datetime.utcnow()

    # Access control settings should remain stable but can change occasionally
    if random.random() < 0.05:  # 5% chance to change access control
        state["accessControl"]["nasa"] = random.choice([True, False])
    if random.random() < 0.05:
        state["accessControl"]["faa"] = random.choice([True, False])
    if random.random() < 0.05:
        state["accessControl"]["safetyAgency"] = random.choice([True, False])
    if random.random() < 0.05:
        state["accessControl"]["aerospaceCompany"] = random.choice([True, False])

    return state

def run_producer():
    config = read_config()
    topic = "security"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "security"
            global state
            state = update_state(state)
            value = {
                "protocol": state["protocol"],
                "encryptionLevel": state["encryptionLevel"],  # Assuming encryption level doesn't change
                "accessControl": state["accessControl"]
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
