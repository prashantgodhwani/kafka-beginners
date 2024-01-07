package org.example;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Simple Kafka Consumer");

        String groupId = "my-java-app";
        String topic = "demo_java";

        //create Producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        //consumer props
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");

        //partitioner assignment - cooperative
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        //get reference to main thread
        final Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, lets exit by calling consumer.wakeup()....");
            consumer.wakeup();

            try{
                mainThread.join();
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }));

        try {
            //subscribe to topic
            consumer.subscribe(Arrays.asList(topic));

            //poll for data
            while (true) {
                log.info("Polling");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("key = " + record.key() + ", value = " + record.value());
                    log.info("Partition = " + record.partition() + ", Offset = " + record.offset());
                }
            }
        }catch (WakeupException wakeupException){
            log.info("Consumer gracefully shutting down...");
        }catch (Exception e){
            log.error("Caught unexpected exception", e);
        }finally {
            consumer.close();
            log.info("Offsets committed and consumer has now shut down gracefully!");
        }

    }
}
