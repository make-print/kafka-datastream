from confluent_kafka import Producer
import time
import signal
import json
import random
from datetime import datetime
from utils import read_config

# Define event types and their details
event_templates = [
    {
        "eventType": "System Check",
        "details": [
            "Checking communication systems.",
            "Running diagnostics on hydraulic systems.",
            "Performing pre-launch system integrity checks.",
            "Verifying fuel levels and pressures."
        ]
    },
    {
        "eventType": "Launch",
        "details": [
            "Ignition sequence start.",
            "Liftoff! We have a liftoff.",
            "Stage separation confirmed.",
            "Payload deployment successful."
        ]
    },
    {
        "eventType": "Abort",
        "details": [
            "Launch abort sequence initiated.",
            "Mission control has called for an abort.",
            "Engine shutdown and abort procedures active.",
            "Safe abort confirmed, all systems nominal."
        ]
    }
]


def generate_event():
    event_template = random.choice(event_templates)
    event_detail = random.choice(event_template["details"])
    event = {
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "eventType": event_template["eventType"],
        "details": event_detail
    }
    return event


def run_producer():
    config = read_config()
    topic = "events"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "events"
            value = [generate_event()]
            producer.produce(topic, key=key, value=json.dumps(value))
            print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")

            producer.poll(0)
            time.sleep(5)
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
