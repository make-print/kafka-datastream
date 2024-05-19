from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaException


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
