import subprocess
import signal
import time
from producers.utils import ensure_topics_exist

producer_scripts = [
    "producers/launch_site_producer.py",
    "producers/weather_data_producer.py",
    "producers/launch_vehicle_producer.py",
    "producers/payload_producer.py",
    "producers/communication_systems_producer.py",
    "producers/security_producer.py",
    "producers/pad_infrastructure_producer.py",
    "producers/operational_status_producer.py",
    "producers/events_producer.py",
    "producers/weather_forecast_hrs_producer.py"
]

topics = [
    "launchSite",
    "weatherData",
    "launchVehicle",
    "payload",
    "communicationSystems",
    "security",
    "padInfrastructure",
    "operationalStatus",
    "events",
    "weatherForecastHrs"
]

processes = []
online_producers = []
failed_producers = []


def log_status():
    """
    Log the status of all producers
    :return:
    """
    print(f"| {'Producer':<40} | {'Status':<10} |")
    print("|" + "-" * 53 + "|")
    for producer, status in online_producers:
        print(f"| {producer:<40} | {status:<10} |")
    for producer in failed_producers:
        print(f"| {producer:<40} | {'FAILED':<10} |")
    print(f"\nTotal Producers: {len(producer_scripts)}")
    print(f"Online Producers: {len(online_producers)}")
    print(f"Failed Producers: {len(failed_producers)}\n")


def start_producers():
    """
    Start all producers
    :return:
    """
    ensure_topics_exist(topics)
    try:
        for script in producer_scripts:
            print(f"Starting {script}...")
            try:
                process = subprocess.Popen(["python", script])
                processes.append((script, process))
                online_producers.append((script, 'ONLINE'))
                print(f"{script} | ONLINE")
            except Exception as e:
                print(f"{script} | FAILED | Error: {e}")
                failed_producers.append(script)

            # Short delay to avoid overwhelming the system
            time.sleep(1)

        log_status()

        # Wait for all processes to complete
        for script, process in processes:
            process.wait()

    except KeyboardInterrupt:
        print("\nShutting down all producers...")
        for script, process in processes:
            process.send_signal(signal.SIGINT)
        for script, process in processes:
            process.wait()
        print("All producers shut down.")
        log_status()


if __name__ == "__main__":
    start_producers()
