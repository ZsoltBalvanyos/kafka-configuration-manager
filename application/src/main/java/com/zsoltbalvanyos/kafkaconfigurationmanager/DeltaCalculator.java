package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static java.util.stream.Collectors.*;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor
public class DeltaCalculator {

  final CurrentState currentState;
  final RequiredState requiredState;

  public List<Acl> aclsToCreate() {
    return requiredState.getAcls().stream()
        .filter(acl -> !currentState.getAcls().contains(acl))
        .collect(toList());
  }

  public List<Acl> aclsToDelete() {
    return currentState.getAcls().stream()
        .filter(acl -> !requiredState.getAcls().contains(acl))
        .collect(toList());
  }

  public List<RequiredTopic> topicsToCreate() {
    return requiredState.getTopics().stream()
        .filter(topic -> notContains(currentState.getTopics(), topic.getName()))
        .collect(toList());
  }

  public List<ExistingTopic> topicsToDelete() {
    return currentState.getTopics().stream()
        .filter(topic -> notContains(requiredState.getTopics(), topic.getName()))
        .collect(toList());
  }

  private boolean notContains(Collection<? extends Identified> topics, Id<?> topicName) {
    return !topics.stream().map(Identified::getName).collect(toList()).contains(topicName);
  }

  public Map<TopicName, Integer> partitionUpdate() {

    var currentStateMap =
        currentState.getTopics().stream()
            .collect(toMap(ExistingTopic::getName, ExistingTopic::getPartitions));

    Map<TopicName, Integer> result = new HashMap<>();

    requiredState.getTopics().stream()
        .filter(topic -> alreadyExists(currentState.getTopics(), topic))
        .forEach(
            topic ->
                topic
                    .getPartitionCount()
                    .ifPresent(
                        partitionCount -> {
                          if (partitionCount < currentStateMap.get(topic.getName()).size()) {
                            throw new RuntimeException(
                                String.format(
                                    "Number of partitions cannot be lowered. Current number of partitions: %d, requested number of partitions: %d",
                                    currentStateMap.get(topic.getName()).size(), partitionCount));
                          }
                          if (partitionCount > currentStateMap.get(topic.getName()).size()) {
                            result.put(
                                topic.getName(),
                                topic
                                    .getPartitionCount()
                                    .orElseGet(this::getDefaultPartitionCount));
                          }
                        }));

    return result;
  }

  public Map<TopicName, List<Partition>> replicationUpdate() {
    Map<TopicName, List<Partition>> result = new HashMap<>();

    requiredState.getTopics().stream()
        .filter(topic -> alreadyExists(currentState.getTopics(), topic))
        .forEach(
            topic -> {
              List<Partition> partitions =
                  IntStream.range(
                          0, currentState.getTopicMap().get(topic.getName()).getPartitions().size())
                      .filter(
                          partition ->
                              currentState
                                      .getTopicMap()
                                      .get(topic.getName())
                                      .getPartitions()
                                      .get(PartitionNumber.of(partition))
                                      .size()
                                  != topic
                                      .getReplicationFactor()
                                      .orElseGet(this::getDefaultReplicationFactor))
                      .mapToObj(
                          partition ->
                              new Partition(
                                  PartitionNumber.of(partition),
                                  selectBrokersForReplication(
                                      currentState
                                          .getTopicMap()
                                          .get(topic.getName())
                                          .getPartitions()
                                          .get(PartitionNumber.of(partition)),
                                      topic
                                          .getReplicationFactor()
                                          .orElseGet(this::getDefaultReplicationFactor))))
                      .collect(Collectors.toList());

              if (!partitions.isEmpty()) {
                result.put(topic.getName(), partitions);
              }
            });

    return result;
  }

  public Map<TopicName, Map<String, Optional<String>>> topicConfigUpdate() {
    Map<TopicName, Map<String, Optional<String>>> result = new HashMap<>();

    Map<TopicName, Map<String, String>> currentConfig =
        currentState.getTopics().stream()
            .collect(toMap(ExistingTopic::getName, ExistingTopic::getConfig));

    Map<TopicName, Map<String, String>> requiredConfig =
        requiredState.getTopics().stream()
            .collect(toMap(RequiredTopic::getName, RequiredTopic::getConfig));

    requiredState.getTopics().stream()
        .filter(topic -> alreadyExists(currentState.getTopics(), topic))
        .map(
            topic ->
                new ConfigUpdate(
                    topic.getName(),
                    currentConfig.get(topic.getName()),
                    requiredConfig.get(topic.getName())))
        .forEach(
            configUpdate -> {
              List<String> configNames = new ArrayList<>();
              configNames.addAll(configUpdate.oldConfig.keySet());
              configNames.addAll(configUpdate.newConfig.keySet());

              Map<String, Optional<String>> configToApply = new HashMap<>();
              configNames.forEach(
                  configName -> {
                    if (configUpdate.oldConfig.containsKey(configName)
                        && !configUpdate.newConfig.containsKey(configName)) {
                      configToApply.put(configName, Optional.empty());
                    } else if (!configUpdate
                        .oldConfig
                        .getOrDefault(configName, UUID.randomUUID().toString())
                        .equals(configUpdate.newConfig.get(configName))) {
                      configToApply.put(
                          configName, Optional.ofNullable(configUpdate.newConfig.get(configName)));
                    }
                  });
              if (!configToApply.isEmpty()) {
                result.put(configUpdate.topicName, configToApply);
              }
            });

    return result;
  }

  public Map<BrokerId, Map<String, Optional<String>>> brokerConfigUpdate() {
    Map<BrokerId, Map<String, Optional<String>>> result = new HashMap<>();

    var requiredBrokerConfig = requiredState.getBrokers();

    currentState
        .getBrokers()
        .forEach(
            broker -> {
              Map<String, Optional<String>> configToApply = new HashMap<>();

              /*
               * Reset config value to the default if the config has been removed from yaml file
               */
              broker.getConfig().values().stream()
                  .filter(config -> !requiredBrokerConfig.containsKey(config.getName()))
                  .filter(config -> !config.isDefault())
                  .filter(config -> !config.isReadOnly())
                  .forEach(config -> configToApply.put(config.getName(), Optional.empty()));

              /*
               * Update config values
               */
              broker.getConfig().values().stream()
                  .filter(config -> requiredBrokerConfig.containsKey(config.getName()))
                  .filter(config -> !config.isDefault())
                  .filter(
                      config ->
                          !requiredBrokerConfig.get(config.getName()).equals(config.getValue()))
                  .forEach(
                      config ->
                          configToApply.put(
                              config.getName(),
                              Optional.ofNullable(requiredBrokerConfig.get(config.getName()))));

              result.put(broker.getId(), configToApply);
            });

    return result;
  }

  public List<Integer> selectBrokersForReplication(
      Collection<Integer> utilizedBrokers, int replicationFactor) {

    var allBrokers =
        currentState.getBrokers().stream()
            .map(Broker::getId)
            .map(Id::get)
            .map(Integer::valueOf)
            .collect(toList());

    if (!allBrokers.containsAll(utilizedBrokers)) {
      throw new RuntimeException(
          String.format(
              "Invalid replication state - Available Brokers: %s, Used Brokers: %s",
              allBrokers.toString(), utilizedBrokers.toString()));
    }
    if (replicationFactor < 1) {
      throw new RuntimeException("Replication factor must be greater than 0");
    }
    if (replicationFactor > allBrokers.size()) {
      throw new RuntimeException(
          String.format(
              "Replication factor [%d] must not be greater than the number of brokers [%d]",
              replicationFactor, allBrokers.size()));
    }

    if (replicationFactor > utilizedBrokers.size()) {
      List<Integer> availableBrokers = new ArrayList<>(allBrokers);
      availableBrokers.removeAll(utilizedBrokers);
      Collections.shuffle(availableBrokers);
      List<Integer> result =
          availableBrokers.subList(0, replicationFactor - utilizedBrokers.size());
      result.addAll(utilizedBrokers);
      return result;
    } else {
      List<Integer> currentStateMutable = new ArrayList<>(utilizedBrokers);
      Collections.shuffle(currentStateMutable);
      return currentStateMutable.subList(0, replicationFactor);
    }
  }

  private boolean alreadyExists(
      Collection<ExistingTopic> existingTopics, RequiredTopic requiredTopic) {
    return existingTopics.stream()
        .map(ExistingTopic::getName)
        .collect(toList())
        .contains(requiredTopic.getName());
  }

  private Integer getDefaultReplicationFactor() {
    return currentState.getBrokers().stream()
        .map(Broker::getConfig)
        .map(config -> config.get("default.replication.factor"))
        .filter(Objects::nonNull)
        .map(BrokerConfig::getValue)
        .map(Integer::valueOf)
        .max(Integer::compareTo)
        .orElse(1);
  }

  private Integer getDefaultPartitionCount() {
    return currentState.getBrokers().stream()
        .map(Broker::getConfig)
        .map(config -> config.get("num.partitions"))
        .filter(Objects::nonNull)
        .map(BrokerConfig::getValue)
        .map(Integer::valueOf)
        .max(Integer::compareTo)
        .orElse(1);
  }

  @Value
  private static class ConfigUpdate {
    TopicName topicName;
    Map<String, String> oldConfig;
    Map<String, String> newConfig;
  }
}
