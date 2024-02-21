package com.codehunter.kafka_pure_playground;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerWithCallback {
    public static final Logger log = LogManager.getLogger(KafkaProducerWithCallback.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        try (KafkaProducer<Long, String> producer = new KafkaProducer<>(properties)) {
            ProducerRecord<Long, String> message = new ProducerRecord<>("order", System.currentTimeMillis(), "[KafkaProducerWithCallback] My order from java");
            producer.send(message, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Metadata: {}", metadata);
                    } else {
                        log.error("Exception: {}", exception.getMessage(), exception);
                    }
                }
            });
            producer.flush();
            producer.close();
        }
        log.info("END!!!");
    }
}