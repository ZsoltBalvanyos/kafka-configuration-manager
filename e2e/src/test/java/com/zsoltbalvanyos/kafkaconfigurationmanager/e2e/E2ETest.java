package com.zsoltbalvanyos.kafkaconfigurationmanager.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class E2ETest {

  public static KafkaContainer kafka;
  public static Admin admin;

  @BeforeAll
  private static void setup() throws Exception {
    kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.4.3"));
    kafka.start();
    admin =
        Admin.create(
            Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()));
  }

  @AfterAll
  private static void cleanup() {
    kafka.stop();
  }

  @Test
  public void test() throws Exception {

    new GenericContainer<>(DockerImageName.parse("kafka-configuration-manager:latest"))
        .dependsOn(kafka)
        .waitingFor(Wait.forLogMessage(".*(Kafka Configuration Manager exited).*", 1))
        .withEnv(Map.of("BOOTSTRAP_SERVER", "172.17.0.3:9092"))
        .withFileSystemBind(
            "./src/test/resources/plaintext.properties", "/properties/command-config.properties")
        .withFileSystemBind("./src/test/resources/test-config.yml", "/config/configuration.yml")
        .withCommand("apply")
        .start();

    var result = admin.listTopics().listings().get().stream();
    var topicNames = result.map(TopicListing::name).collect(Collectors.toList());

    assertThat(topicNames).containsExactlyInAnyOrder("orders.pizza.0", "orders.coffee.2");
    assertThat(getConfigEntries("orders.pizza.0").get("flush.messages").value()).isEqualTo("100");
    assertThat(getConfigEntries("orders.pizza.0").get("unclean.leader.election.enable").value())
        .isEqualTo("false");
  }

  private Config getConfigEntries(String topicName)
      throws ExecutionException, InterruptedException {
    var configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    return admin.describeConfigs(List.of(configResource)).all().get().get(configResource);
  }
}
