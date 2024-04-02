package com.codehunter.kafka_pure_playground;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.lifecycle.Startable;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class KafkaContainerCluster implements Startable {
    Logger log = LogManager.getLogger(KafkaContainerCluster.class);
    private final Network network;
    private final GenericContainer<?> zookeeper;
    private final KafkaContainer kafka;

    public KafkaContainerCluster() {
        this.network = Network.newNetwork();
        this.zookeeper = new GenericContainer<>(DockerImageName.parse("confluentinc/cp-zookeeper").withTag("7.3.6"))
                .withNetwork(this.network)
                .withNetworkAliases("zookeeper")
                .withEnv("ZOOKEEPER_CLIENT_PORT", String.valueOf(KafkaContainer.ZOOKEEPER_PORT));

        int internalTopicsRf = 1;
        this.kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka").withTag("7.3.6"))
                .withNetwork(this.network)
                .withNetworkAliases("kafka")
                .dependsOn(this.zookeeper)
                .withExternalZookeeper("zookeeper:" + KafkaContainer.ZOOKEEPER_PORT)
                .withEnv("KAFKA_BROKER_ID", 1 + "")
                .withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", internalTopicsRf + "")
                .withEnv("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", internalTopicsRf + "")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", internalTopicsRf + "")
                .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", internalTopicsRf + "")
                .withStartupTimeout(Duration.ofMinutes(1));
    }

    @Override
    public void start() {
        this.kafka.start();

        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofSeconds(2))
                .untilAsserted(() -> {
                    Container.ExecResult result =
                            this.zookeeper.execInContainer(
                                    "sh",
                                    "-c",
                                    "zookeeper-shell localhost:" +
                                    KafkaContainer.ZOOKEEPER_PORT +
                                    " ls /brokers/ids | tail -n 1"
                            );
                    String brokers = result.getStdout();
                    String error = result.getStderr();
                    log.info("=====Zookeeper=====: {}", brokers);
                    assertThat(brokers != null && brokers.split(",").length == 1).isTrue();
                    assertThat(error).isEmpty();
                });

        log.info("=====Kafka START=====: {}");
    }

    public String getBootstrapServers() {
        return this.kafka.getBootstrapServers();
//        return Optional.of(this.kafka).stream().map(KafkaContainer::getBootstrapServers).collect(Collectors.joining(","));
    }

    @Override
    public void stop() {
        this.kafka.stop();
    }
}
