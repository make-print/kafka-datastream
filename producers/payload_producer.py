from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config, get_payload_data


# # Initialize the state
# state = {
#     "latitude": 28.5721,
#     "longitude": -80.648,
#     "altitude": 550.0,
#     "rfFrequencies": [3.5, 4.0],
#     "temperature": 20.0,
#     "weight": 250.0,
#     "fuelLevel": {
#         "CH4": 1.0,
#         "LOX": 1.0
#     }
# }
#
#
# def update_state(state):
#     # Simulate position change
#     state["latitude"] += random.uniform(-0.001, 0.001)
#     state["longitude"] += random.uniform(-0.001, 0.001)
#
#     # Simulate altitude change
#     state["altitude"] += random.uniform(-0.5, 0.5)
#
#     # Simulate temperature change
#     state["temperature"] += random.uniform(-0.1, 0.1)
#
#     # Simulate weight change
#     state["weight"] -= random.uniform(0.1, 0.5)  # weight decreases over time
#
#     # Simulate fuel level change
#     state["fuelLevel"]["CH4"] -= random.uniform(0.01, 0.05)
#     state["fuelLevel"]["LOX"] -= random.uniform(0.01, 0.05)
#
#     # Ensure fuel levels do not go below 0
#     state["fuelLevel"]["CH4"] = max(state["fuelLevel"]["CH4"], 0)
#     state["fuelLevel"]["LOX"] = max(state["fuelLevel"]["LOX"], 0)
#
#     return state
#
#
# def run_producer():
#     config = read_config()
#     topic = "payload"
#
#     producer = Producer(config)
#
#     def shutdown(signum, frame):
#         print("\nShutting down producer...")
#         producer.flush()
#         exit(0)
#
#     signal.signal(signal.SIGINT, shutdown)
#
#     try:
#         while True:
#             key = "payload"
#             global state
#             state = update_state(state)
#             value = {
#                 "id": "PL789",
#                 "type": "Satellite",
#                 "status": {
#                     "health": random.choice(["Operational", "Non-Operational"]),
#                     "position": {
#                         "latitude": round(state["latitude"], 6),
#                         "longitude": round(state["longitude"], 6),
#                         "altitude": round(state["altitude"], 2)
#                     },
#                     "telemetry": {
#                         "rfFrequencies": state["rfFrequencies"],
#                         "temperature": round(state["temperature"], 2),
#                         "weight": round(state["weight"], 2),
#                         "fuelLevel": {
#                             "CH4": round(state["fuelLevel"]["CH4"], 2),
#                             "LOX": round(state["fuelLevel"]["LOX"], 2)
#                         }
#                     },
#                     "items": {
#
#                     }
#                 }
#             }
#             producer.produce(topic, key=key, value=json.dumps(value))
#             print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")
#
#             producer.poll(0)
#             time.sleep(5)
#     except Exception as e:
#         print(f"An error occurred: {e}")
#         producer.flush()

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
