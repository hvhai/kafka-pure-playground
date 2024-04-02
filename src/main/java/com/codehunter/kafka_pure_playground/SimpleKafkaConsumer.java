package com.codehunter.kafka_pure_playground;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class SimpleKafkaConsumer {
    public static final Logger log = LogManager.getLogger(SimpleKafkaConsumer.class);
//    public static final String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
//    public static final String BOOTSTRAP_SERVER = "localhost:58673";

    private final String bootstrapServer;
    private final String groupId;
    private final String mode;
    private final String topic;
    private final Cache<String, String> cache;

    private final KafkaConsumer<Long, String> consumer;

    public SimpleKafkaConsumer(String bootstrapServer, String groupId, String mode, String topics, Cache<String, String> cache) {
        this.bootstrapServer = bootstrapServer;
        this.groupId = groupId;
        this.mode = mode;
        this.cache = cache;
        this.topic = topics;

        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, mode);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic));
    }

    public static void main(String[] args) {
        Cache<String, String> cache = Caffeine.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .maximumSize(100)
                .build();
        SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(
                BOOTSTRAP_SERVER, UUID.randomUUID().toString(), "earliest", "order", cache);
        consumer.listen();
    }

    public void listen() {

        log.info("Start listening");
        try {
            while (true) {
                ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<Long, String> record : records) {
                    log.info("Key: {}, Value: {}", record.key(), record.value());
                    log.info("Partition: {}, Offset: {}", record.partition(), record.offset());
                    cache.put(record.key().toString(), record.value());
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
