package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.ExecutionPlan;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@AllArgsConstructor
public class Executor {

  final Logger log = LoggerFactory.getLogger(getClass().getName());
  final KafkaClient kafkaClient;
  final DeltaCalculator deltaCalculator;
  final List<Callable<String>> rollbacks = new ArrayList<>();

  public void run(ExecutionPlan plan, Collection<Model.Broker> brokers) throws Exception {

    try {
      if (!plan.getAclsToDelete().isEmpty()) {
        // TODO rollback
        kafkaClient.deleteAcls(plan.getAclsToDelete());
        log.info("ACLs have been deleted successfully");
      }

      if (!plan.getAclsToCreate().isEmpty()) {
        // TODO rollback
        kafkaClient.createAcls(plan.getAclsToCreate());
        log.info("New ACLs have been added successfully");
      }

      if (!plan.getBrokerConfigurationChanges().isEmpty()) {
        rollbacks.add(rollbackBrokerConfig(brokers, plan.getBrokerConfigurationChanges()));
        kafkaClient.updateConfigOfBrokers(plan.getBrokerConfigurationChanges());
        log.info("Broker configurations have been updated successfully.");
      }

      if (!plan.getTopicConfigurationChanges().isEmpty()) {
        rollbacks.add(
            rollbackConfig(plan.getOriginalConfigs(), plan.getTopicConfigurationChanges()));
        kafkaClient.updateConfigOfTopics(plan.getTopicConfigurationChanges());
        log.info("Topic configurations have been updated successfully.");
      }

      if (!plan.getTopicsToCreate().isEmpty()) {
        rollbacks.add(rollbackCreate(plan.getTopicsToCreate()));
        kafkaClient.createTopics(plan.getTopicsToCreate());
        log.info("Topics have been created successfully.");
      }

      if (!plan.getReplicationChanges().isEmpty()) {
        rollbacks.add(
            rollbackReplication(plan.getOriginalPartitions(), plan.getReplicationChanges()));
        kafkaClient.updateReplication(plan.getReplicationChanges());
        log.info("Replication factors have been updated successfully.");
      }

      if (!plan.getPartitionChanges().isEmpty() && kafkaClient.noOngoingReassignment()) {
        kafkaClient.updatePartitions(plan.getPartitionChanges());
        log.info("Partition counts have been updated successfully.");
      }

      try {
        if (!plan.getTopicsToDelete().isEmpty()) {
          kafkaClient.deleteTopics(
              plan.getTopicsToDelete().stream()
                  .map(Model.ExistingTopic::getName)
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

  public Callable<String> rollbackCreate(Collection<Model.Topic> createdTopics) {
    return () -> {
      kafkaClient.deleteTopics(
          createdTopics.stream().map(Model.Topic::getName).collect(Collectors.toSet()));
      return "Rolled back new topic creation";
    };
  }

  public Callable<String> rollbackConfig(
      Map<String, Map<String, String>> originalConfigs,
      Map<String, Map<String, Optional<String>>> configChanges) {

    Map<String, Map<String, Optional<String>>> rollback = new HashMap<>();

    configChanges.forEach(
        (topicName, config) -> {
          Map<String, Optional<String>> topicConfig = new HashMap<>();
          originalConfigs
              .get(topicName)
              .forEach((name, value) -> topicConfig.put(name, Optional.ofNullable(value)));
          rollback.put(topicName, topicConfig);
        });

    return () -> {
      kafkaClient.updateConfigOfTopics(rollback);
      return "Rolled back topic configuration changes";
    };
  }

  public Callable<String> rollbackBrokerConfig(
      Collection<Model.Broker> brokers, Map<String, Map<String, Optional<String>>> configChanges) {

    Map<String, Map<String, Model.BrokerConfig>> originalConfigs = new HashMap<>();
    brokers.forEach(
        broker -> originalConfigs.put(String.valueOf(broker.getId()), broker.getConfig()));

    Map<String, Map<String, Optional<String>>> rollback = new HashMap<>();

    configChanges.forEach(
        (brokerId, config) -> {
          Map<String, Optional<String>> brokerConfig = new HashMap<>();
          originalConfigs
              .get(brokerId)
              .forEach(
                  (name, value) -> brokerConfig.put(name, Optional.ofNullable(value.getValue())));
          rollback.put(brokerId, brokerConfig);
        });

    return () -> {
      kafkaClient.updateConfigOfBrokers(rollback);
      return "Rolled back broker configuration changes";
    };
  }

  public Callable<String> rollbackReplication(
      Map<String, Map<Integer, Integer>> originalPartitions,
      Map<String, List<Model.Partition>> replicationChanges) {
    Map<String, List<Model.Partition>> rollback = new HashMap<>();

    replicationChanges.forEach(
        (topicName, replicationChange) -> {
          List<Model.Partition> rollbackTo =
              replicationChange.stream()
                  .map(
                      partition ->
                          new Model.Partition(
                              partition.getPartitionNumber(),
                              deltaCalculator.selectBrokersForReplication(
                                  partition.getReplicas(),
                                  originalPartitions
                                      .get(topicName)
                                      .get(partition.getPartitionNumber()))))
                  .collect(Collectors.toList());

          rollback.put(topicName, rollbackTo);
        });

    return () -> {
      kafkaClient.updateReplication(rollback);
      return "Rolled back replication changes";
    };
  }
}
