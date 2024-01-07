package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Simple Kafka Producer With Keys");

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //producer props
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for(int i = 0; i < 10; i++) {

            String topic = "demo_java";
            String key = "id_" + i;
            String value = "Hello World " + i;

            //create a producer record
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

            //send data
            producer.send(producerRecord, (metadata, exception) -> {
                //executed every time a record successfully sent or exception thrown
                if (exception == null) {
                    log.info("Received new metadata \n" +
                            "Topic: " + metadata.topic() + "\n" +
                            "Key: " + key + "\n" +
                            "Partition: " + metadata.partition() + "\n" +
                            "Offset: " + metadata.offset() + "\n" +
                            "Timestamp: " + metadata.timestamp()
                    );
                } else {
                    log.error("Error while producing", exception);
                }
            });
        }

        //flush and close producer
        producer.flush();
        producer.close();
    }
}
