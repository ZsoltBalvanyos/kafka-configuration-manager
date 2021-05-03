package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import org.junit.Test;

public class ReporterTest {

  Reporter reporter = new Reporter();

  private String readFile(String file) throws IOException {
    return Files.readString(Path.of("src/test/resources/" + file));
  }

  @Test
  public void test() throws IOException {
    Collection<Model.ExistingTopic> existingTopics =
        List.of(
            new Model.ExistingTopic(
                "topic-4",
                List.of(
                    new Model.Partition(2, List.of(0, 1)),
                    new Model.Partition(4, List.of(0, 1, 2)),
                    new Model.Partition(6, List.of(0, 1, 3))),
                Map.of("key-3", "value-3")),
            new Model.ExistingTopic(
                "topic-14",
                List.of(new Model.Partition(4, List.of(0, 1))),
                Map.of("key-3", "value-3")),
            new Model.ExistingTopic(
                "topic-54",
                List.of(new Model.Partition(4, List.of(0, 1))),
                Map.of("key-3", "value-3")));

    Collection<Model.Broker> brokers =
        List.of(
            new Model.Broker(
                1,
                Map.of(
                    "key-1", new Model.BrokerConfig("key-1", "value-1", true, true),
                    "key-2", new Model.BrokerConfig("key-2", "value-2", false, true),
                    "key-3", new Model.BrokerConfig("key-3", "value-3", true, false),
                    "key-4", new Model.BrokerConfig("key-4", "value-4", false, false))));

    Collection<Model.Acl> acls =
        List.of(
            new Model.Acl(
                "resource-type-1",
                "name-1",
                "pattern-type-1",
                List.of(
                    new Model.Permission("principal-1", "host-1", "operation-1", "type-1"),
                    new Model.Permission("principal-2", "host-2", "operation-2", "type-2"))),
            new Model.Acl(
                "resource-type-6",
                "name-6",
                "pattern-type-6",
                List.of(
                    new Model.Permission("principal-6", "host-6", "operation-6", "type-6"),
                    new Model.Permission("principal-2", "host-2", "operation-2", "type-2"))));

    assertThat(reporter.print(existingTopics, brokers, acls))
        .isEqualTo(readFile("describe-output.txt"));
  }

  @Test
  public void test2() throws IOException {
    Model.ExecutionPlan executionPlan =
        new Model.ExecutionPlan(
            // original topic configurations
            Map.of(
                "topic-1", Map.of("key-1", "value-1"),
                "topic-2", Map.of("key-1", "value-1")),

            // original partition settings
            Map.of(
                "topic-1", Map.of(0, 1),
                "topic-2", Map.of(1, 1)),

            // replication changes
            Map.of(
                "topic-1", List.of(new Model.Partition(0, List.of(0, 1))),
                "topic-2", List.of(new Model.Partition(2, List.of(0, 1)))),

            // partition changes
            Map.of(
                "topic-1", 20,
                "topic-2", 30),

            // topic configuration changes
            Map.of(
                "topic-1", Map.of("key-1", Optional.of("new-value-1")),
                "topic-2", Map.of("key-2", Optional.of("new-value-2"))),

            // topics to create
            List.of(
                new Model.Topic(
                    "topic-3", Optional.of(3), Optional.of(3), Map.of("key-3", "value-3")),
                new Model.Topic(
                    "topic-5", Optional.of(3), Optional.of(3), Map.of("key-3", "value-3")),
                new Model.Topic(
                    "topic-8", Optional.of(3), Optional.of(3), Map.of("key-3", "value-3"))),

            // topics to delete
            List.of(
                new Model.ExistingTopic(
                    "topic-4",
                    List.of(
                        new Model.Partition(2, List.of(0, 1)),
                        new Model.Partition(4, List.of(0, 1, 2)),
                        new Model.Partition(6, List.of(0, 1, 3))),
                    Map.of("key-3", "value-3")),
                new Model.ExistingTopic(
                    "topic-14",
                    List.of(new Model.Partition(4, List.of(0, 1))),
                    Map.of("key-3", "value-3")),
                new Model.ExistingTopic(
                    "topic-54",
                    List.of(new Model.Partition(4, List.of(0, 1))),
                    Map.of("key-3", "value-3"))),

            // broker configuration changes
            Map.of(
                "1", Map.of("key-1", Optional.of("new-value-1")),
                "2",
                    Map.of(
                        "key-2",
                        Optional.of("new-value-2"),
                        "key-3",
                        Optional.of("new-value-3"),
                        "key-4",
                        Optional.of("new-value-4"))),

            // acls to create
            List.of(
                new Model.Acl(
                    "resource-type-1",
                    "name-1",
                    "pattern-type-1",
                    List.of(
                        new Model.Permission("principal-1", "host-1", "operation-1", "type-1"),
                        new Model.Permission("principal-2", "host-2", "operation-2", "type-2"))),
                new Model.Acl(
                    "resource-type-6",
                    "name-6",
                    "pattern-type-6",
                    List.of(
                        new Model.Permission("principal-6", "host-6", "operation-6", "type-6"),
                        new Model.Permission("principal-2", "host-2", "operation-2", "type-2")))),

            // acls to delete
            List.of(
                new Model.Acl(
                    "resource-type-2",
                    "name-2",
                    "pattern-type-2",
                    List.of(
                        new Model.Permission("principal-3", "host-3", "operation-3", "type-3"),
                        new Model.Permission("principal-4", "host-4", "operation-4", "type-4"))),
                new Model.Acl(
                    "resource-type-9",
                    "name-9",
                    "pattern-type-9",
                    List.of(
                        new Model.Permission("principal-1", "host-1", "operation-1", "type-1"),
                        new Model.Permission("principal-2", "host-2", "operation-2", "type-2")))));

    assertThat(reporter.print(executionPlan)).isEqualTo(readFile("plan-output.txt"));
  }
}
