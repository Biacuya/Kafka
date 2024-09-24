// serviceA/consumerA.js
const fs = require("fs");
const Kafka = require("node-rdkafka");

function readConfig(fileName) {
    const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
    return data.reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key.trim()] = value.trim(); // Trim whitespace
        }
        return config;
    }, {});
}

function consume(topic, config) {
    config["group.id"] = "group-a";
    const topicConfig = { "auto.offset.reset": "earliest" };
    const consumer = new Kafka.KafkaConsumer(config, topicConfig);
    consumer.connect();

    consumer.on("ready", () => {
        consumer.subscribe([topic]);
        consumer.consume();
    });

    consumer.on("data", (message) => {
        console.log(`Consumed message from topic ${message.topic}: key = ${message.key.toString()} value = ${message.value.toString()}`);
    });

    consumer.on("error", (err) => {
        console.error("Error in consumer:", err);
    });
}

function main() {
    const config = readConfig("client.properties");
    const topic = "test_topic_A"; // Topic específico para este servicio
    consume(topic, config);
}

main();
