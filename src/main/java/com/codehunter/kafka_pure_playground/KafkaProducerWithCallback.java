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
    public static final String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        KafkaProducerWithCallback kafkaProducerWithCallback = new KafkaProducerWithCallback();
        kafkaProducerWithCallback.send("Message refactor", BOOTSTRAP_SERVER);
    }

    public void send(String messageValue, String bootstrapServer) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);
        try {
            String value = String.format("[KafkaProducerWithCallback] %s", messageValue);
            ProducerRecord<Long, String> message = new ProducerRecord<>("order", System.currentTimeMillis(), value);
            producer.send(message, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception == null) {
                        log.info("Record with Key:{}, Value:{} was sent to Offset:{}, Partition:{}", message.key(), message.value(), metadata.offset(), metadata.partition());
                    } else {
                        log.error("Exception: {}", exception.getMessage(), exception);
                    }
                }
            });
        } finally {
            producer.flush();
            producer.close();
        }
        log.info("END!!!");
    }
}