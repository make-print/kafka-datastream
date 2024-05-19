from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config, get_payload_data

def transform_payload_data(raw_payloads):
    transformed_data = []
    for raw in raw_payloads:
        transformed = {
            "id": raw["id"],
            "type": "Satellite",  # Assuming type is always Satellite
            "status": {
                "health": "Operational" if int(raw["id"]) % 2 == 0 else "Non-Operational",
                "position": {
                    "latitude": float(raw["lat"]),
                    "longitude": float(raw["lon"]),
                    "altitude": 550.0  # Placeholder altitude
                },
                "telemetry": {
                    "rfFrequencies": [float(raw["rf"])],
                    "temperature": float(raw["temp"]),
                    "weight": float(raw["weight"]),
                    "fuelLevel": {
                        "CH4": 0.5,  # Placeholder fuel level for CH4
                        "LOX": 0.5  # Placeholder fuel level for LOX
                    }
                }
            }
        }
        transformed_data.append(transformed)
    return transformed_data


def run_producer():
    config = read_config()
    topic = "payload"

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "payload"
            raw_payloads = get_payload_data()
            payloads = transform_payload_data(raw_payloads)
            producer.produce(topic, key=key, value=json.dumps(payloads))
            print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(payloads):12}")

            producer.poll(0)
            time.sleep(60)  # Adjust sleep time as needed
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
