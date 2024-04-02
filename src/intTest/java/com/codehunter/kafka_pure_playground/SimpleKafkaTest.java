package com.codehunter.kafka_pure_playground;


import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.awaitility.Awaitility.await;

class SimpleKafkaTest {
    public static final Logger log = LogManager.getLogger(SimpleKafkaTest.class);

    @Test
    void testConsumer() throws InterruptedException, ExecutionException, TimeoutException {
        try (KafkaContainerCluster kafkaContainerCluster = new KafkaContainerCluster();) {
            kafkaContainerCluster.start();
            String boostrapServer = kafkaContainerCluster.getBootstrapServers();
            log.info("bootstrap: " + boostrapServer);
            String topic = "int-test-topic";

            try (Admin adminClient = Admin.create(ImmutableMap.of(
                    AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer));
                 KafkaProducer<Long, String> producer = new KafkaProducer<>(
                         ImmutableMap.of(
                                 ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer,
                                 ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()
                         ),
                         new LongSerializer(),
                         new StringSerializer());
            ) {
                // given
                // topic
                Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topic, 1, (short) 1));
                adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);
                Thread.sleep(5000);

                // consumer is listening
                Cache<String, String> cache = Caffeine.newBuilder()
                        .expireAfterWrite(1, TimeUnit.MINUTES)
                        .maximumSize(100)
                        .build();
                Runnable runnable = () -> {
                    String groupId = UUID.randomUUID().toString();
                    SimpleKafkaConsumer simpleKafkaConsumer = new SimpleKafkaConsumer(
                            boostrapServer, groupId, "earliest", topic, cache);
                    simpleKafkaConsumer.listen();
                };
                Thread thread = new Thread(runnable);
                thread.start();
                Thread.sleep(2000);

                // when message is sent
                log.info("sent message");
                long key = System.currentTimeMillis();
                String messageValue = "int test message value";
                String value = String.format("[SimpleKafka][%s] %s",
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(key), ZoneId.systemDefault()), messageValue);
                ProducerRecord<Long, String> message = new ProducerRecord<>(topic, key, value);

                RecordMetadata metadata = producer.send(message).get();
                log.info("Record with Key:{}, Value:{} was sent to Offset:{}, Partition:{}",
                        message.key(), message.value(), metadata.offset(), metadata.partition());

                // then message is consume
                await().atMost(Duration.ofSeconds(90))
                        .pollInterval(Duration.ofSeconds(2))
                        .untilAsserted(() -> {
                            var cachedMessage = cache.getIfPresent(String.valueOf(key));
                            assertThat(cachedMessage).isNotEmpty();
                            assertThat(cachedMessage).contains(messageValue);
                        });
            }
        }
    }

    @Test
    void testKafkaProducer() throws Exception {
        try (KafkaContainerCluster cluster = new KafkaContainerCluster()) {
            cluster.start();
            String bootstrapServers = cluster.getBootstrapServers();

            testKafkaProducerFunctionality(bootstrapServers, 1, 1);
        }
    }

    protected void testKafkaProducerFunctionality(String bootstrapServers, int partitions, int rf) throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(
                        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                );
                KafkaConsumer<Long, String> consumer = new KafkaConsumer<>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new LongDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "messages";
            log.info("bootstrap: " + bootstrapServers);
            consumer.subscribe(Collections.singletonList(topicName));

            SimpleKafkaProducer simpleKafkaProducer = new SimpleKafkaProducer();
            String message = "Test message for kafka producer";
            simpleKafkaProducer.send(message, bootstrapServers, topicName);

            await().atMost(
                    10,
                    TimeUnit.SECONDS).untilAsserted(
                    () -> {
                        ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(100));

                        assertThat(records).isNotEmpty();
                        log.info("records not empty");

                        assertThat(records)
                                .hasSize(1);
                        ConsumerRecord<Long, String> result = records.iterator().next();

                        assertThat(result.topic()).isEqualTo(topicName);
                        assertThat(result.value()).contains(message);

                    }
            );

            consumer.unsubscribe();
        }
    }

    @Test
    void testKafkaContainerCluster() throws Exception {
        try (KafkaContainerCluster cluster = new KafkaContainerCluster()) {
            cluster.start();
            String bootstrapServers = cluster.getBootstrapServers();

            testKafkaFunctionality(bootstrapServers, 1, 1);
        }
    }

    protected void testKafkaFunctionality(String bootstrapServers, int partitions, int rf) throws Exception {
        try (
                AdminClient adminClient = AdminClient.create(
                        ImmutableMap.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
                );
                KafkaProducer<String, String> producer = new KafkaProducer<>(
                        ImmutableMap.of(
                                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ProducerConfig.CLIENT_ID_CONFIG,
                                UUID.randomUUID().toString()
                        ),
                        new StringSerializer(),
                        new StringSerializer()
                );
                KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
                        ImmutableMap.of(
                                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                                bootstrapServers,
                                ConsumerConfig.GROUP_ID_CONFIG,
                                "tc-" + UUID.randomUUID(),
                                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                                "earliest"
                        ),
                        new StringDeserializer(),
                        new StringDeserializer()
                );
        ) {
            String topicName = "messages";

            log.info("bootstrap: " + bootstrapServers);
            Collection<NewTopic> topics = Collections.singletonList(new NewTopic(topicName, partitions, (short) rf));
            adminClient.createTopics(topics).all().get(30, TimeUnit.SECONDS);

            consumer.subscribe(Collections.singletonList(topicName));

            producer.send(new ProducerRecord<>(topicName, "testcontainers", "rulezzz")).get();

            await().atMost(
                    10,
                    TimeUnit.SECONDS).untilAsserted(
                    () -> {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                        assertThat(records)
                                .hasSize(1)
                                .extracting(ConsumerRecord::topic, ConsumerRecord::key, ConsumerRecord::value)
                                .containsExactly(tuple(topicName, "testcontainers", "rulezzz"));

                    }
            );

            consumer.unsubscribe();
        }
    }
}
