// serviceA/consumerA.js
const fs = require("fs");
const Kafka = require("node-rdkafka");

function readConfig(fileName) {
    const data = fs.readFileSync(fileName, "utf8").toString().split("\n");
    return data.reduce((config, line) => {
        const [key, value] = line.split("=");
        if (key && value) {
            config[key.trim()] = value.trim(); 
        }
        return config;
    }, {});
}

function produce(topic, config) {
    const key = "saludo_desde_b";
    const value = "Hola A";
  
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
    config["group.id"] = "group-request";
    const topicConfig = { "auto.offset.reset": "latest" };
    const consumer = new Kafka.KafkaConsumer(config, topicConfig);
    consumer.connect();

    consumer.on("ready", () => {
        consumer.subscribe([topic]);
        consumer.consume();
    });

    consumer.on("data", (message) => {
        const key = message.key ? message.key.toString() : null; // Manejar cuando la key sea null
        const value = message.value ? message.value.toString() : null; // Manejar cuando el value sea null
        console.log(`Consumed message from topic ${message.topic}: key = ${key} value = ${value}`);
    });

    consumer.on("error", (err) => {
        console.error("Error in consumer:", err);
    });
}



// Iniciar el consumidor en el servicio B
const config = readConfig("client.properties");
const topic = "request_topic";
const topic1 = "response_topic";
consume(topic, config);
produce(topic1, config);
