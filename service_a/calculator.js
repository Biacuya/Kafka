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

function produce(topic, config) {
    const key = "saludo_desde_a";
    const value = "Hola B";

    const producer = new Kafka.Producer(config);
    producer.connect();

    producer.on("ready", () => {
        producer.produce(topic, -1, Buffer.from(value), Buffer.from(key));
        console.log(`Produced message to topic ${topic}: key = ${key} value = ${value}`);
    });

    producer.on("error", (err) => {
        console.error("Error in producer:", err);
    });
}

function consume(topic, config) {
    config["group.id"] = "group-response";
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

// Ejemplo de uso
const config = readConfig("client.properties");
const topic = "response_topic";
const topic1 = "request_topic";
produce(topic1, config);
consume(topic, config);
