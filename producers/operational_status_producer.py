from confluent_kafka import Producer
import time
import signal
import json
import random
from datetime import datetime, timedelta
from utils import read_config

# Initialize state
state = {
    "systemRangeCheck": True,
    "operationalWindows": [
        {
            "startTime": datetime.utcnow(),
            "endTime": datetime.utcnow() + timedelta(hours=1),
            "status": "Scheduled"
        }
    ],
    "systems": {
        "comms": "Operational",
        "power": "Operational",
        "propulsion": "Operational",
        "navigation": "Operational"
    }
}


def update_state(state):
    # Randomly change system statuses to simulate issues and recoveries
    for system in state["systems"]:
        if random.random() < 0.1:  # 10% chance to change status
            state["systems"][system] = random.choice(["Operational", "Degraded", "Offline"])

    # Update the operational window
    if state["operationalWindows"]:
        current_window = state["operationalWindows"][-1]
        if datetime.utcnow() > current_window["endTime"]:
            # End the current window and start a new one
            new_start_time = datetime.utcnow()
            state["operationalWindows"].append({
                "startTime": new_start_time,
                "endTime": new_start_time + timedelta(hours=1),
                "status": "Scheduled"
            })

    return state


def run_producer():
    config = read_config()
    topic = "operationalStatus"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "operationalStatus"
            global state
            state = update_state(state)

            current_window = state["operationalWindows"][-1]
            value = {
                "currentWindowEndTime": current_window["endTime"].isoformat() + "Z",
                "systemRangeCheck": state["systemRangeCheck"],
                "systems": state["systems"],
                "operationalWindows": [
                    {
                        "startTime": win["startTime"].isoformat() + "Z",
                        "endTime": win["endTime"].isoformat() + "Z",
                        "status": win["status"]
                    }
                    for win in state["operationalWindows"]
                ]
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
