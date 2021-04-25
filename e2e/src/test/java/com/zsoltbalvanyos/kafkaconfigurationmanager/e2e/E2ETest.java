package com.zsoltbalvanyos.kafkaconfigurationmanager.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.*;
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

  @Test
  public void test() throws Exception {
    Properties properties = new Properties();
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9093");
    Admin admin = Admin.create(properties);

    new GenericContainer<>(DockerImageName.parse("kafka-configuration-manager:latest"))
        .waitingFor(Wait.forLogMessage(".*(Kafka Configuration Manager exited).*", 1))
        .withEnv("BOOTSTRAP_SERVER", kafkaEndpoint)
        .withFileSystemBind(
            "./src/test/resources/plaintext.properties", "/properties/command-config.properties")
        .withFileSystemBind("./src/test/resources/test-config.yml", "/config/configuration.yml")
        .withCommand("apply")
        .withLogConsumer(new Slf4jLogConsumer(log))
        .withNetworkMode("host")
        .start();

    List<String> topicNames =
        admin.listTopics().listings().get().stream()
            .map(TopicListing::name)
            .collect(Collectors.toList());

    assertThat(topicNames).containsExactlyInAnyOrder("orders.pizza.0", "orders.coffee.2");
    assertThat(getConfigEntries("orders.pizza.0", admin).get("flush.messages").value())
        .isEqualTo("100");
    assertThat(
            getConfigEntries("orders.pizza.0", admin).get("unclean.leader.election.enable").value())
        .isEqualTo("false");
  }

  private Config getConfigEntries(String topicName, Admin admin)
      throws ExecutionException, InterruptedException {
    ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
    return admin.describeConfigs(Arrays.asList(configResource)).all().get().get(configResource);
  }
}
