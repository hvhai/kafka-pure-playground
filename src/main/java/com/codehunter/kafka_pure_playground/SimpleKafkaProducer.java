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
    public static final String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";

    public void send(String messageValue, String bootstrapServer) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);

        try {
            long key = System.currentTimeMillis();
            String value = String.format("[SimpleKafka][%s] %s", LocalDateTime.ofInstant(Instant.ofEpochMilli(key), ZoneId.systemDefault()), messageValue);
            ProducerRecord<Long, String> message = new ProducerRecord<>("order", key, value);
            RecordMetadata metadata = producer.send(message).get();
            log.info("Record with Key:{}, Value:{} was sent to Offset:{}, Partition:{}", message.key(), message.value(), metadata.offset(), metadata.partition());
        } catch (InterruptedException ie) {
            log.error("InterruptedException: ", ie);
            Thread.currentThread().interrupt();
        } catch (ExecutionException ee) {
            log.error("ExecutionException: ", ee);
        } finally {
            producer.flush();
            producer.close();
            log.info("after closing producer");
        }
    }

    public static void main(String[] args) {
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();
        simpleKafkaProducer.send("Refactor message for kafka consumer", BOOTSTRAP_SERVER);
    }
}