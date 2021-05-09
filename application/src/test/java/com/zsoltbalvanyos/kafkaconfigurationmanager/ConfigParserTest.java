package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static org.assertj.core.api.Assertions.assertThat;

import io.vavr.collection.HashMap;
import io.vavr.collection.List;
import io.vavr.collection.Traversable;
import java.util.Optional;
import org.junit.Test;

public class ConfigParserTest {
  ConfigParser configParser = new ConfigParser("./src/test/resources/schema-test.yml");

  @Test
  public void configFileDeserializableToConfigurationModel() {
    Configuration configuration = configParser.getConfiguration();
    Traversable<RequiredTopic> result = configParser.getRequiredState(configuration);

    assertThat(configuration.getBrokerConfig())
        .isEqualTo(
            HashMap.of(
                "log.cleaner.threads", "3",
                "sasl.kerberos.service.name", "kerberos"));

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                new Model.RequiredTopic(
                    Model.TopicName.of("topic-1"),
                    Optional.of(2),
                    Optional.of(1),
                    HashMap.of(
                        "flush.messages", "1",
                        "segment.index.bytes", "20",
                        "cleanup.policy", "compact")),
                new Model.RequiredTopic(
                    Model.TopicName.of("topic-2"),
                    Optional.of(3),
                    Optional.of(5),
                    HashMap.of(
                        "flush.messages", "60",
                        "segment.index.bytes", "100")),
                new Model.RequiredTopic(
                    Model.TopicName.of("topic-3"),
                    Optional.of(2),
                    Optional.of(2),
                    HashMap.of(
                        "flush.messages", "1",
                        "segment.index.bytes", "20",
                        "cleanup.policy", "delete"))));

    assertThat(configuration.getAcls())
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                new Model.Acl(
                    "TOPIC",
                    "orders",
                    "PREFIXED",
                    List.of(
                        new Model.Permission("User:alice", "localhost", "ALL", "ALLOW"),
                        new Model.Permission("User:bob", "localhost", "ALL", "ALLOW")))));
  }
}
