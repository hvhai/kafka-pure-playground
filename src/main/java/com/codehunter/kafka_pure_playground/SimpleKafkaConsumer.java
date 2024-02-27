package com.codehunter.kafka_pure_playground;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

public class SimpleKafkaConsumer {
    public static final Logger log = LogManager.getLogger(SimpleKafkaConsumer.class);
    public static final String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        listen(BOOTSTRAP_SERVER, "groupId", "earliest", Collections.singletonList("order"));
    }

    private static void listen(String bootstrapServer, String groupId, String mode, Collection<String> topics) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, mode);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(topics);

        try {
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<Long, String> record : records) {
                    log.info("Key: {}, Value: {}", record.key(), record.value());
                    log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                }
            }
        } catch (Exception exception) {
            log.error("ExecutionException: ", exception);
        } finally {
            consumer.close();
            log.info("after closing consumer");

        }
    }
}
