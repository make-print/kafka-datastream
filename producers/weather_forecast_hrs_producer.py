from confluent_kafka import Producer
import time
import signal
import json
import random
from utils import read_config, read_hourly_forecast


def run_producer():
    config = read_config()
    topic = "weatherForecast"
    weather_file = "weather/weather_trim.json"  # Path to your JSON file

    producer = Producer(config)

    def shutdown(signum, frame):
        print("\nShutting down producer...")
        producer.flush()
        exit(0)

    signal.signal(signal.SIGINT, shutdown)

    try:
        while True:
            key = "weatherForecast"
            forecasts = read_hourly_forecast(weather_file)
            for value in forecasts:
                producer.produce(topic, key=key, value=json.dumps(value))
                print(f"Produced message to topic {topic}: key = {key:12} value = {json.dumps(value):12}")
                producer.poll(0)
                time.sleep(1)  # Adjust sleep time as needed
    except Exception as e:
        print(f"An error occurred: {e}")
        producer.flush()


if __name__ == "__main__":
    run_producer()
