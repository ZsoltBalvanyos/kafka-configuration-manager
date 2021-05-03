package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.stream.Collectors.*;

import java.util.Collection;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class Reporter {

  private static final String TAB = "\t";
  private static final String NEWLINE = System.lineSeparator();
  private static final String INDENTED_NEWLINE = NEWLINE + TAB;
  private static final String DOUBLE_INDENTED_NEWLINE = INDENTED_NEWLINE + TAB;

  public String print(
      Collection<ExistingTopic> existingTopics, Collection<Broker> brokers, Collection<Acl> acls) {
    StringBuilder sb = new StringBuilder();

    if (!brokers.isEmpty()) {
      sb.append(getHeader("Current broker configuration"));
      brokers.forEach(
          broker -> {
            sb.append("Broker ").append(broker.getId());
            getSortedMap(broker.getConfig())
                .forEach(
                    (name, value) -> {
                      sb.append(INDENTED_NEWLINE);
                      sb.append(name).append(" -> ").append(value.getValue());
                    });
            sb.append(NEWLINE);
          });
    }

    if (existingTopics.isEmpty()) {
      sb.append("No topic found").append(NEWLINE);
    } else {
      sb.append(getHeader("Current state of existing topics"));
      existingTopics.stream().map(this::printTopic).forEach(sb::append);
    }

    if (acls.isEmpty()) {
      sb.append("No ACL found").append(NEWLINE);
    } else {
      sb.append(getHeader("Current ACL configurations"));
      sb.append(printAcls(acls));
    }

    return sb.toString();
  }

  private <K extends Comparable<K>, V> SortedMap<K, V> getSortedMap(Map<K, V> map) {
    SortedMap<K, V> result = new TreeMap<>();
    map.keySet().stream().sorted().forEach(key -> result.put(key, map.get(key)));
    return result;
  }

  public String print(ExecutionPlan plan) {
    StringBuilder sb = new StringBuilder();

    if (!plan.getBrokerConfigurationChanges().isEmpty()) {
      sb.append(getHeader("Broker configuration changes"));
      getSortedMap(plan.getBrokerConfigurationChanges())
          .forEach(
              (brokerId, config) -> {
                sb.append("Broker ").append(brokerId);
                getSortedMap(config)
                    .forEach(
                        (name, value) -> {
                          sb.append(INDENTED_NEWLINE)
                              .append(name)
                              .append(" -> ")
                              .append(value.orElse("default"));
                        });
                sb.append(NEWLINE);
              });
    }

    if (!plan.getAclsToCreate().isEmpty()) {
      sb.append(getHeader("ACLs to create")).append(printAcls(plan.getAclsToCreate()));
    }

    if (!plan.getAclsToDelete().isEmpty()) {
      sb.append(getHeader("ACLs to delete")).append(printAcls(plan.getAclsToDelete()));
    }

    if (!plan.getReplicationChanges().isEmpty()) {
      sb.append(getHeader("Changes in replication"));

      getSortedMap(plan.getReplicationChanges())
          .forEach(
              (topicName, partition) -> {
                sb.append(topicName);
                partition.forEach(
                    p ->
                        sb.append(INDENTED_NEWLINE)
                            .append("partition[")
                            .append(p.getPartitionNumber())
                            .append("]: ")
                            .append(
                                plan.getOriginalPartitions()
                                    .get(topicName)
                                    .get(p.getPartitionNumber()))
                            .append(" -> ")
                            .append(p.getReplicas().size()));
                sb.append(NEWLINE);
              });
    }

    if (!plan.getPartitionChanges().isEmpty()) {
      sb.append(getHeader("Changes in partition count"));

      getSortedMap(plan.getPartitionChanges())
          .forEach(
              (topicName, partitionCount) ->
                  sb.append(topicName)
                      .append(": ")
                      .append(plan.getOriginalPartitions().get(topicName).size())
                      .append(" -> ")
                      .append(partitionCount)
                      .append(NEWLINE));
    }

    if (!plan.getTopicConfigurationChanges().isEmpty()) {
      sb.append(getHeader("Changes in topic configuration"));

      getSortedMap(plan.getTopicConfigurationChanges())
          .forEach(
              (topicName, partition) -> {
                sb.append(topicName);
                partition.forEach(
                    (name, value) ->
                        sb.append(INDENTED_NEWLINE)
                            .append(name)
                            .append(": ")
                            .append(
                                plan.getOriginalConfigs()
                                    .get(topicName)
                                    .getOrDefault(name, "default"))
                            .append(" -> ")
                            .append(value.orElse("default")));
                sb.append(NEWLINE);
              });
    }

    if (!plan.getTopicsToCreate().isEmpty()) {
      sb.append(getHeader("New topics to create"));
      plan.getTopicsToCreate().stream().map(this::printTopic).forEach(sb::append);
    }

    if (!plan.getTopicsToDelete().isEmpty()) {
      sb.append(getHeader("New topics to delete"));
      plan.getTopicsToDelete().stream().map(this::printTopic).forEach(sb::append);
    }

    return sb.append(NEWLINE).toString();
  }

  private String getHeader(String header) {
    return NEWLINE + NEWLINE + header + ':' + NEWLINE + "-".repeat(header.length()) + NEWLINE;
  }

  private String printTopic(Topic topic) {
    return topic.getName()
        + INDENTED_NEWLINE
        + "Partition count: "
        + topic.getPartitionCount().map(String::valueOf).orElse("default")
        + INDENTED_NEWLINE
        + "Replication factor: "
        + topic.getReplicationFactor().map(String::valueOf).orElse("default")
        + printMap(topic.getConfig(), INDENTED_NEWLINE)
        + NEWLINE;
  }

  private String printTopic(ExistingTopic topic) {
    StringBuilder sb =
        new StringBuilder()
            .append(topic.getName())
            .append(printMap(topic.getConfig(), INDENTED_NEWLINE))
            .append(INDENTED_NEWLINE)
            .append("Partitions:");
    topic
        .getPartitions()
        .forEach(
            partition -> {
              sb.append(DOUBLE_INDENTED_NEWLINE)
                  .append(
                      String.format(
                          "Id: %d, replicas: [%s]",
                          partition.getPartitionNumber(),
                          partition.getReplicas().stream()
                              .map(String::valueOf)
                              .collect(joining(","))));
            });
    return sb.append(NEWLINE).toString();
  }

  private <K, V> String printMap(Map<K, V> map, String separator) {
    StringBuilder sb = new StringBuilder();
    map.forEach(
        (name, value) -> {
          sb.append(separator);
          sb.append(name).append(": ").append(value);
        });
    return sb.toString();
  }

  private String printAcls(Collection<Acl> acls) {
    StringBuilder sb = new StringBuilder();

    acls.forEach(
        acl -> {
          sb.append("Resource type: ")
              .append(acl.getResourceType())
              .append(NEWLINE)
              .append("Name: ")
              .append(acl.getName())
              .append(NEWLINE)
              .append("Pattern type: ")
              .append(acl.getPatternType());

          acl.getPermissions()
              .forEach(
                  permission ->
                      sb.append(DOUBLE_INDENTED_NEWLINE)
                          .append("Principal: ")
                          .append(permission.getPrincipal())
                          .append(DOUBLE_INDENTED_NEWLINE)
                          .append("Host: ")
                          .append(permission.getHost())
                          .append(DOUBLE_INDENTED_NEWLINE)
                          .append("Operation: ")
                          .append(permission.getOperation())
                          .append(DOUBLE_INDENTED_NEWLINE)
                          .append("Permission type: ")
                          .append(permission.getPermissionType()));
          sb.append(NEWLINE);
        });

    return sb.toString();
  }
}
