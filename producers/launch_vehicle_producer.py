from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config, get_vehicle_data


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

            # Fetch vehicle data
            fetched_data = get_vehicle_data()

            # Generate additional data
            generated_data = {
                "status": {
                    "position": {
                        "latitude": 28.5721,
                        "longitude": -80.648,
                        "altitude": round(random.uniform(0.0, 100.0), 2)
                    },
                    "telemetry": {
                        "estimatedTimeToFull": round(random.uniform(0.0, 3600.0), 2)
                    }
                }
            }

            # Merge fetched and generated data
            value = {
                "id": fetched_data.get("id", "LV456"),
                "type": fetched_data.get("type", "Rocket"),
                "status": {
                    "health": fetched_data.get("health", random.choice(["Good", "Fair", "Poor"])),
                    "position": {
                        "latitude": float(fetched_data.get("lat", generated_data["status"]["position"]["latitude"])),
                        "longitude": float(fetched_data.get("lon", generated_data["status"]["position"]["longitude"])),
                        "altitude": float(fetched_data.get("alt", generated_data["status"]["position"]["altitude"]))
                    },
                    "telemetry": {
                        "pressure": float(fetched_data.get("pressure", random.uniform(900.0, 1100.0))),
                        "voltage": float(fetched_data.get("voltage", random.uniform(20.0, 30.0))),
                        "heartbeat": fetched_data.get("heartbeat", random.choice([True, False])).lower() == 'true',
                        "fuelConsumption": float(fetched_data.get("fuelconsumption", random.uniform(0.0, 10.0))),
                        "fuelLevel": {
                            "CH4": float(fetched_data.get("ch4", 0.0)),
                            "LOX": float(fetched_data.get("lox", 0.0))
                        },
                        "estimatedTimeToFull": generated_data["status"]["telemetry"]["estimatedTimeToFull"]
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
