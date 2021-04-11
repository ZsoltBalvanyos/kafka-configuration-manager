package com.zsoltbalvanyos.kafkaconfigurationmanager.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

public class E2ETest {

  private final Logger log = LoggerFactory.getLogger(getClass().getName());
  private final String kafkaEndpoint = "10.5.0.5:9093";
  private final Admin admin =
      Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093"));

  @Test
  public void test() throws Exception {

    new GenericContainer<>(DockerImageName.parse("kafka-configuration-manager:latest"))
        .waitingFor(Wait.forLogMessage(".*(Kafka Configuration Manager exited).*", 1))
        .withEnv(Map.of("BOOTSTRAP_SERVER", kafkaEndpoint))
        .withFileSystemBind(
            "./src/test/resources/plaintext.properties", "/properties/command-config.properties")
        .withFileSystemBind("./src/test/resources/test-config.yml", "/config/configuration.yml")
        .withCommand("apply")
        .withLogConsumer(new Slf4jLogConsumer(log))
        .withNetworkMode("host")
        .start();

    var topicNames =
        admin.listTopics().listings().get().stream()
            .map(TopicListing::name)
            .collect(Collectors.toList());

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
