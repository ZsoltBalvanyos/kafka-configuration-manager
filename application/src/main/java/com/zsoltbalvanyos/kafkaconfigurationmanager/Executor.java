package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import java.util.Arrays;
import java.util.Optional;
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

  final List<Callable<String>> rollbacks = List.of();

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
        rollbacks.append(
            rollbackBrokerConfig(
                deltaCalculator.currentState.getBrokers(), plan.getBrokerConfigurationChanges()));
        commands.updateConfigOfBrokers(plan.getBrokerConfigurationChanges());
        log.info("Broker configurations have been updated successfully.");
      }

      if (!plan.getTopicConfigurationChanges().isEmpty()) {
        rollbacks.append(
            rollbackTopicConfig(currentState.getTopics(), plan.getTopicConfigurationChanges()));
        commands.updateConfigOfTopics(plan.getTopicConfigurationChanges());
        log.info("Topic configurations have been updated successfully.");
      }

      if (!plan.getTopicsToCreate().isEmpty()) {
        rollbacks.append(rollbackTopicCreate(plan.getTopicsToCreate()));
        commands.createTopics(plan.getTopicsToCreate());
        log.info("Topics have been created successfully.");
      }

      if (!plan.getReplicationChanges().isEmpty()) {
        rollbacks.append(
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
              plan.getTopicsToDelete().map(ExistingTopic::getName).map(TopicName::get));
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

  public Callable<String> rollbackTopicCreate(Traversable<RequiredTopic> createdTopics) {
    return () -> {
      commands.deleteTopics(createdTopics.map(RequiredTopic::getName).map(TopicName::get));
      return "Rolled back new topic creation";
    };
  }

  public Callable<String> rollbackTopicConfig(
      Traversable<ExistingTopic> originalTopics,
      Map<TopicName, Map<String, Optional<String>>> configChanges) {

    Map<TopicName, Map<String, Optional<String>>> rollback =
        configChanges.map(
            (topicName, config) ->
                Map.entry(
                    topicName,
                    originalTopics
                        .toMap(ExistingTopic::getName, ExistingTopic::getConfig)
                        .get(topicName)
                        .get()
                        .map((name, value) -> Map.entry(name, Optional.ofNullable(value)))));

    return () -> {
      commands.updateConfigOfTopics(rollback);
      return "Rolled back topic configuration changes";
    };
  }

  public Callable<String> rollbackBrokerConfig(
      Traversable<Broker> originalBrokers,
      Map<BrokerId, Map<String, Optional<String>>> configChanges) {

    Map<BrokerId, Map<String, BrokerConfig>> originalConfigs =
        originalBrokers.toMap(Broker::getId, Broker::getConfig);

    Map<BrokerId, Map<String, Optional<String>>> rollback =
        configChanges
            .keySet()
            .toMap(
                brokerId ->
                    Map.entry(
                        brokerId,
                        originalConfigs
                            .get(brokerId)
                            .get()
                            .map(
                                (name, value) ->
                                    Map.entry(name, Optional.ofNullable(value.getValue())))));

    return () -> {
      commands.updateConfigOfBrokers(rollback);
      return "Rolled back broker configuration changes";
    };
  }

  public Callable<String> rollbackReplication(
      Map<TopicName, ExistingTopic> topicMap,
      Map<TopicName, Traversable<Partition>> replicationChanges) {

    Map<TopicName, Traversable<Partition>> rollback =
        replicationChanges.map(
            (topicName, replicationChange) ->
                Map.entry(
                    topicName,
                    replicationChange.map(
                        partition ->
                            new Partition(
                                partition.getPartitionNumber(),
                                deltaCalculator.selectBrokersForReplication(
                                    partition.getReplicas(),
                                    topicMap
                                        .get(topicName)
                                        .get()
                                        .getPartitions()
                                        .get(partition.getPartitionNumber())
                                        .get()
                                        .size())))));

    return () -> {
      commands.updateReplication(rollback);
      return "Rolled back replication changes";
    };
  }
}
