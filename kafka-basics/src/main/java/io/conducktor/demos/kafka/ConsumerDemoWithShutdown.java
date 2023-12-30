package io.conducktor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer Demo");

        String groupId = "my-java-app";
        String topic = "demo_java";

        // connect to localhost properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // setting consumer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset", "earliest"); // other options none/latest/earliest

        // create kafka producer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // get reference to main thread
        final Thread mainThread = Thread.currentThread();

        // adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, exiting by calling consumer.wakeup()...");
                consumer.wakeup();

                // joining main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        // poll for data
        try {
            consumer.subscribe(Arrays.asList(topic));

            while(true) {
                log.info("Polling for data");

                // waiting 1sec before polling
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record: records) {
                    log.info("Key: " + record.key() + " , Value: " + record.value());
                    log.info("Partition: " + record.partition() + ", Offset: " + record.offset());
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch(Exception e) {
            log.info("Unexpected exception in the consumer");
            e.printStackTrace();
        } finally {
           // closing consumer and committing the offsets
           consumer.close();
           log.info("Consumer is shutdown gracefully.");
        }

    }
}

