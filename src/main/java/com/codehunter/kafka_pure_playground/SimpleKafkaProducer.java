package com.codehunter.kafka_pure_playground;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SimpleKafkaProducer {
    public static final Logger log = LogManager.getLogger(SimpleKafkaProducer.class);
//    public static final String BOOTSTRAP_SERVER = "localhost:9092,localhost:9093,localhost:9094";
    public static final String BOOTSTRAP_SERVER = "localhost:9092";
//    public static final String BOOTSTRAP_SERVER = "localhost:59465";

    public void send(String messageValue, String bootstrapServer, String topic) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<Long, String> producer = new KafkaProducer<>(properties);


        try {
            createTopicIfNotExist(topic, bootstrapServer);

            long key = System.currentTimeMillis();
            String value = String.format("[SimpleKafka][%s] %s",
                    LocalDateTime.ofInstant(Instant.ofEpochMilli(key), ZoneId.systemDefault()), messageValue);
            ProducerRecord<Long, String> message = new ProducerRecord<>(topic, key, value);
            RecordMetadata metadata = producer.send(message).get();
            log.info("Record with Key:{}, Value:{} was sent to Offset:{}, Partition:{}",
                    message.key(), message.value(), metadata.offset(), metadata.partition());
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

    private void createTopicIfNotExist(String topic, String bootstrapServer) {
        try (
                Admin adminClient = Admin.create(Map.of(
                        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer));
        ) {
            Collection<NewTopic> topics = Collections.singletonList(
                    new NewTopic(topic, 1, (short) 1)
                            .configs(Collections.singletonMap(
                                    TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)));

            ListTopicsOptions options = new ListTopicsOptions();
            options.listInternal(true);
            ListTopicsResult listTopicsResult = adminClient.listTopics(options);
            Set<String> currentTopicList = listTopicsResult.names().get();
            boolean hasTopic = currentTopicList.stream().anyMatch(topic::equalsIgnoreCase);
            if (hasTopic) {
                CreateTopicsResult createTopicsResult = adminClient.createTopics(topics);
                KafkaFuture<Void> voidKafkaFuture = createTopicsResult.values().get(topic);
                voidKafkaFuture.get(30, TimeUnit.SECONDS);
            }
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();
        simpleKafkaProducer.send("Refactor message for kafka consumer", BOOTSTRAP_SERVER, "order");
    }
}