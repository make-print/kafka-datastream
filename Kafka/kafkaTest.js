"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var kafkaConfig_1 = require("./kafkaConfig");
var node_rdkafka_1 = require("node-rdkafka");
function main() {
    // producer and consumer code here
    var config = (0, kafkaConfig_1.readConfig)("client.properties");
    var topic = "hardtech";
    var key = "key";
    var value = "value";
    // creates a new producer instance
    var producer = new node_rdkafka_1.default.Producer(config);
    producer.connect();
    producer.on("ready", function () {
        // produces a sample message
        producer.produce(topic, -1, Buffer.from(value), Buffer.from(key));
        console.log("Produced message to topic ".concat(topic, ": key = ").concat(key, " value = ").concat(value));
    });
    // set the kafkaTest's group ID, offset and initialize it
    config["group.id"] = "nodejs-group-1";
    var topicConfig = { "auto.offset.reset": "earliest" };
    var kafkaTest = new node_rdkafka_1.default.KafkaConsumer(config, topicConfig);
    kafkaTest.connect();
    kafkaTest
        .on("ready", function () {
        // subscribe to the topic and start polling for messages
        kafkaTest.subscribe([topic]);
        kafkaTest.consume();
    })
        .on("data", function (message) {
        // print incoming messages
        console.log("Consumed message from topic ".concat(message.topic, ": key = ").concat(message.key.toString(), " value = ").concat(message.key.toString()));
    });
}
main();
