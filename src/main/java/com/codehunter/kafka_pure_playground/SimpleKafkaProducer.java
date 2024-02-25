package com.codehunter.kafka_pure_playground;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleKafkaProducer {
    public static final Logger log = LogManager.getLogger(SimpleKafkaProducer.class);

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        try {
            long key = System.currentTimeMillis();
            String value = String.format("[SimpleKafka][%s] My first order from java", LocalDateTime.ofInstant(Instant.ofEpochMilli(key), ZoneId.systemDefault()));
            ProducerRecord<Long, String> message = new ProducerRecord<>("order", key, value);
            RecordMetadata metadata = producer.send(message).get();
            log.info("Record with Key:{}, Value:{} was sent to Offset:{}, Partition:{}", message.key(), message.value(), metadata.offset(), metadata.partition());
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception: {}", e.getMessage(), e);
        } finally {
            producer.flush();
            producer.close();
        }
    }
}