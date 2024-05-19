import requests
import json
from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient, NewTopic


def read_current_weather(file_path):
    """
    Read the current weather from a JSON file.
    :param file_path: Path to the JSON file containing weather data.
    :return: Dictionary with current weather data.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)

    current_weather = data["current"]

    weather_data = {
        "timestamp": current_weather["dt"],
        "sunrise": current_weather["sunrise"],
        "sunset": current_weather["sunset"],
        "temperature": current_weather["temp"],
        "pressure": current_weather["pressure"],
        "humidity": current_weather["humidity"],
        "dewPoint": current_weather["dew_point"],
        "uvIndex": current_weather["uvi"],
        "clouds": current_weather["clouds"],
        "visibility": current_weather["visibility"],
        "windSpeed": current_weather["wind_speed"],
        "windDirection": current_weather["wind_deg"],
        "windGust": current_weather.get("wind_gust", None),  # wind_gust might be optional
        "weatherMain": current_weather["weather"][0]["main"],
        "weatherDescription": current_weather["weather"][0]["description"],
        "lightningDetected": "lightning" in [weather["main"].lower() for weather in current_weather["weather"]]
    }

    return weather_data


def read_hourly_forecast(file_path):
    """
    Read the hourly weather forecast from a JSON file.
    :param file_path: Path to the JSON file containing weather data.
    :return: List of dictionaries with hourly weather data.
    """
    with open(file_path, 'r') as file:
        data = json.load(file)

    hourly_forecast = data.get("hourly", [])

    forecasts = []
    for forecast in hourly_forecast:
        hourly_data = {
            "timestamp": forecast.get("dt"),
            "temperature": forecast.get("temp"),
            "feels_like": forecast.get("feels_like"),
            "pressure": forecast.get("pressure"),
            "humidity": forecast.get("humidity"),
            "dewPoint": forecast.get("dew_point"),
            "uvIndex": forecast.get("uvi"),
            "clouds": forecast.get("clouds"),
            "visibility": forecast.get("visibility"),
            "windSpeed": forecast.get("wind_speed"),
            "windDirection": forecast.get("wind_deg"),
            "windGust": forecast.get("wind_gust", None),  # wind_gust might be optional
            "weatherMain": forecast["weather"][0].get("main"),
            "weatherDescription": forecast["weather"][0].get("description"),
            "rainProb": forecast.get("pop", None),  # Probability of precipitation
            "rain1hVolume": forecast.get("rain", {}).get("1h", None)  # Rain volume for last hour
        }
        forecasts.append(hourly_data)

    return forecasts


def read_config():
    """
    Read the configuration for the Kafka producer.
    :return: configuration dictionary
    """
    config = {
        "bootstrap.servers": "pkc-p11xm.us-east-1.aws.confluent.cloud:9092",
        "security.protocol": "sasl_ssl",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "EM2ETYYDZY7L2AID",
        "sasl.password": "zPm3ndcR+71c07LQYxg/24B+uIjQuh1eZ7mwJ0FJa+hTTRMlD3TOcpMVp3o3OYQb",
        "session.timeout.ms": 45000
    }
    return config


def ensure_topics_exist(topics: list):
    """
    Ensure that the specified topics exist in the Kafka cluster.
    :param topics: list of topics to create
    """
    config = read_config()
    admin_client = AdminClient(config)

    existing_topics = set(admin_client.list_topics().topics.keys())
    new_topics = [NewTopic(topic, num_partitions=1, replication_factor=3) for topic in topics if
                  topic not in existing_topics]

    if new_topics:
        fs = admin_client.create_topics(new_topics)

        for topic, f in fs.items():
            try:
                f.result()
                print(f"Topic {topic} created")
            except KafkaException as e:
                print(f"Failed to create topic {topic}: {e}")


def get_payload_data():
    """
    Get the payload data from the launch twin API.
    :return: List of payload data
    """
    url = "https://us-central1-aiot-fit-xlab.cloudfunctions.net/launchtwin"
    headers = {
        'Content-Type': 'application/json'
    }
    all_payloads = []

    for package_id in range(1, 6):
        payload = json.dumps({
            "action": "launchtwin",
            "function": "getpayload",
            "packageid": str(package_id)
        })
        response = requests.post(url, headers=headers, data=payload)
        response_data = response.json()  # Assuming the response is in JSON format
        if 'response' in response_data:
            all_payloads.extend(response_data['response'])  # Extend the list with the payload objects

    return all_payloads


def get_vehicle_data():
    """
    Get the vehicle data from the launch twin API.
    :return:  List of vehicle data
    """

    url = "https://us-central1-aiot-fit-xlab.cloudfunctions.net/launchtwin"
    headers = {
        'Content-Type': 'application/json'
    }

    vehicle = json.dumps({
        "action": "launchtwin",
        "function": "getvehicle",
        "vehicleid": "Starship-SN15"
    })
    response = requests.post(url, headers=headers, data=vehicle)
    response_data = response.json()  # Assuming the response is in JSON format
    if 'response' in response_data:
        return response_data['response'][0]


# Test the function
if __name__ == "__main__":
    # payloads = get_payload_data()
    # print(json.dumps(payloads, indent=2))
    vehicle = get_vehicle_data()
    print(json.dumps(vehicle, indent=2))
