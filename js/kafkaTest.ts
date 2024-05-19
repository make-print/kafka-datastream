import Kafka from "node-rdkafka";

const client_props_path = "C:/Users/the_3/Make-Print-Dev/server/TypeScript/Kafka/client.properties";

export const main = () => {
  console.log("Kafka Test");
  // producer and consumer code here
  // const config = readConfig(client_props_path);
  const config = {

    'metadata.broker.list': process.env.KAFKA_BROKER_LIST,
    'security.protocol': 'sasl_ssl',
    'sasl.mechanisms': process.env.KAFKA_SASL_MECHANISMS,
    'sasl.username': process.env.KAFKA_SASL_USERNAME,
    'sasl.password': process.env.KAFKA_SASL_PASSWORD,
    'group.id': 'nodejs-group-1',
    // other necessary configurations

  };
  const topic = "hardtech";
  const key = "key";
  const value = "value";

  // creates a new producer instance
  const producer = new Kafka.Producer({
    "metadata.broker.list": config["metadata.broker.list"],
    "security.protocol": 'sasl_ssl',
    "sasl.mechanisms": config["sasl.mechanisms"],
    "sasl.username": config["sasl.username"],
    "sasl.password": config["sasl.password"],
  });
  producer.connect();

  producer.on("ready", () => {
    // produces a sample message
    producer.produce(topic, -1, Buffer.from(value), Buffer.from(key));
    console.log(
      `Produced message to topic ${topic}: key = ${key} value = ${value}`
    );
  });

// set the kafkaTest's group ID, offset and initialize it
  config["group.id"] = "nodejs-group-1";
  const topicConfig: any = {"auto.offset.reset": "earliest"};
  const kafkaTest = new Kafka.KafkaConsumer({
    "metadata.broker.list": config["metadata.broker.list"],
    "group.id": config["group.id"],
    "security.protocol": 'sasl_ssl',
    "sasl.mechanisms": config["sasl.mechanisms"],
    "sasl.username": config["sasl.username"],
    "sasl.password": config["sasl.password"],
  }, topicConfig);
  kafkaTest.connect();

  kafkaTest
    .on("ready", () => {
      // subscribe to the topic and start polling for messages
      kafkaTest.subscribe([topic]);
      kafkaTest.consume();
    })
    .on("data", (message: any) => {
      // print incoming messages
      console.log(
        `Consumed message from topic ${
          message.topic
        }: key = ${message.key.toString()} value = ${message.key.toString()}`
      );
    });

}
