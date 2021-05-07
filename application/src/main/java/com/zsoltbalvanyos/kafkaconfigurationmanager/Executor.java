package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.stream.Collectors.toMap;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class Executor {

  final Commands commands;
  final Queries queries;
  final DeltaCalculator deltaCalculator;

  final List<Callable<String>> rollbacks = new ArrayList<>();

  @SneakyThrows
  public void run(ExecutionPlan plan, CurrentState currentState) {

    try {
      if (!plan.getAclsToDelete().isEmpty()) {
        // TODO rollback
        commands.deleteAcls(plan.getAclsToDelete());
        log.info("ACLs have been deleted successfully");
      }

      if (!plan.getAclsToCreate().isEmpty()) {
        // TODO rollback
        commands.createAcls(plan.getAclsToCreate());
        log.info("New ACLs have been added successfully");
      }

      if (!plan.getBrokerConfigurationChanges().isEmpty()) {
        rollbacks.add(
            rollbackBrokerConfig(
                deltaCalculator.currentState.getBrokers(), plan.getBrokerConfigurationChanges()));
        commands.updateConfigOfBrokers(plan.getBrokerConfigurationChanges());
        log.info("Broker configurations have been updated successfully.");
      }

      if (!plan.getTopicConfigurationChanges().isEmpty()) {
        rollbacks.add(
            rollbackConfig(currentState.getTopics(), plan.getTopicConfigurationChanges()));
        commands.updateConfigOfTopics(plan.getTopicConfigurationChanges());
        log.info("Topic configurations have been updated successfully.");
      }

      if (!plan.getTopicsToCreate().isEmpty()) {
        rollbacks.add(rollbackCreate(plan.getTopicsToCreate()));
        commands.createTopics(plan.getTopicsToCreate());
        log.info("Topics have been created successfully.");
      }

      if (!plan.getReplicationChanges().isEmpty()) {
        rollbacks.add(
            rollbackReplication(currentState.getTopicMap(), plan.getReplicationChanges()));
        commands.updateReplication(plan.getReplicationChanges());
        log.info("Replication factors have been updated successfully.");
      }

      if (!plan.getPartitionChanges().isEmpty()) {
        if (queries.ongoingReassignment(15))
          throw new RuntimeException(
              "Failed to update partition count due to ongoing partition reassignment");
        commands.updatePartitions(plan.getPartitionChanges());
        log.info("Partition counts have been updated successfully.");
      }

      try {
        if (!plan.getTopicsToDelete().isEmpty()) {
          commands.deleteTopics(
              plan.getTopicsToDelete().stream()
                  .map(ExistingTopic::getName)
                  .map(TopicName::get)
                  .collect(Collectors.toSet()));
          log.info("Topics have been deleted successfully.");
        }
      } catch (Exception e) {
        log.error("Failed to delete topics due to {}", e.getMessage());
      }

    } catch (Exception e) {
      log.error("Rolling back changes due to: {}", e.getMessage());
      for (Callable<String> rollback : rollbacks) {
        log.warn(rollback.call());
      }
      log.error(
          Arrays.stream(e.getStackTrace())
              .map(StackTraceElement::toString)
              .collect(Collectors.joining("\n\t")));
    }
  }

  public Callable<String> rollbackCreate(Collection<RequiredTopic> createdTopics) {
    return () -> {
      commands.deleteTopics(
          createdTopics.stream()
              .map(RequiredTopic::getName)
              .map(TopicName::get)
              .collect(Collectors.toSet()));
      return "Rolled back new topic creation";
    };
  }

  public Callable<String> rollbackConfig(
      Collection<ExistingTopic> topics,
      Map<TopicName, Map<String, Optional<String>>> configChanges) {

    Map<TopicName, Map<String, Optional<String>>> rollback = new HashMap<>();

    configChanges.forEach(
        (topicName, config) -> {
          Map<String, Optional<String>> topicConfig = new HashMap<>();
          topics.stream()
              .collect(toMap(ExistingTopic::getName, ExistingTopic::getConfig))
              .get(topicName)
              .forEach((name, value) -> topicConfig.put(name, Optional.ofNullable(value)));
          rollback.put(topicName, topicConfig);
        });

    return () -> {
      commands.updateConfigOfTopics(rollback);
      return "Rolled back topic configuration changes";
    };
  }

  public Callable<String> rollbackBrokerConfig(
      Collection<Broker> originalBrokers,
      Map<BrokerId, Map<String, Optional<String>>> configChanges) {

    Map<BrokerId, Map<String, BrokerConfig>> originalConfigs =
        originalBrokers.stream().collect(toMap(Broker::getId, Broker::getConfig));
    Map<BrokerId, Map<String, Optional<String>>> rollback = new HashMap<>();

    configChanges
        .keySet()
        .forEach(
            brokerId -> {
              Map<String, Optional<String>> brokerConfig = new HashMap<>();
              originalConfigs
                  .get(brokerId)
                  .forEach(
                      (name, value) ->
                          brokerConfig.put(name, Optional.ofNullable(value.getValue())));
              rollback.put(brokerId, brokerConfig);
            });

    return () -> {
      commands.updateConfigOfBrokers(rollback);
      return "Rolled back broker configuration changes";
    };
  }

  public Callable<String> rollbackReplication(
      Map<TopicName, ExistingTopic> topicMap, Map<TopicName, List<Partition>> replicationChanges) {

    Map<TopicName, List<Partition>> rollback = new HashMap<>();

    replicationChanges.forEach(
        (topicName, replicationChange) -> {
          List<Partition> rollbackTo =
              replicationChange.stream()
                  .map(
                      partition ->
                          new Partition(
                              partition.getPartitionNumber(),
                              deltaCalculator.selectBrokersForReplication(
                                  partition.getReplicas(),
                                  topicMap
                                      .get(topicName)
                                      .getPartitions()
                                      .get(partition.getPartitionNumber())
                                      .size())))
                  .collect(Collectors.toList());

          rollback.put(topicName, rollbackTo);
        });

    return () -> {
      commands.updateReplication(rollback);
      return "Rolled back replication changes";
    };
  }
}
