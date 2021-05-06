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

  final Collection<Broker> allBrokers;

  public List<Acl> aclsToCreate(Collection<Acl> currentAcls, Collection<Acl> requiredAcls) {
    return requiredAcls.stream().filter(acl -> !currentAcls.contains(acl)).collect(toList());
  }

  public List<Acl> aclsToDelete(Collection<Acl> currentAcls, Collection<Acl> requiredAcls) {
    return currentAcls.stream().filter(acl -> !requiredAcls.contains(acl)).collect(toList());
  }

  public List<Topic> topicsToCreate(
      Collection<ExistingTopic> currentState, Collection<Topic> requiredState) {
    return requiredState.stream()
        .filter(topic -> notContains(currentState, topic.getName()))
        .collect(toList());
  }

  public List<ExistingTopic> topicsToDelete(
      Collection<ExistingTopic> currentState, Collection<Topic> requiredState) {
    return currentState.stream()
        .filter(topic -> notContains(requiredState, topic.getName()))
        .collect(toList());
  }

  private boolean notContains(Collection<? extends Named> topics, String topicName) {
    return !topics.stream().map(Named::getName).collect(toList()).contains(topicName);
  }

  public Map<String, Integer> partitionUpdate(
      Collection<ExistingTopic> currentState, Collection<Topic> requiredState) {
    Map<String, Collection<Partition>> currentStateMap =
        currentState.stream().collect(toMap(ExistingTopic::getName, ExistingTopic::getPartitions));

    Map<String, Integer> result = new HashMap<>();

    requiredState.stream()
        .filter(topic -> alreadyExists(currentState, topic))
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

  public Map<String, List<Partition>> replicationUpdate(
      Collection<ExistingTopic> currentState, Collection<Topic> requiredState) {
    Map<String, List<Partition>> result = new HashMap<>();

    Map<String, Map<Integer, List<Integer>>> currentPartitions =
        currentState.stream()
            .collect(
                toMap(
                    ExistingTopic::getName,
                    e ->
                        e.getPartitions().stream()
                            .collect(
                                toMap(Partition::getPartitionNumber, Partition::getReplicas))));

    requiredState.stream()
        .filter(topic -> alreadyExists(currentState, topic))
        .forEach(
            topic -> {
              List<Partition> partitions =
                  IntStream.range(0, currentPartitions.get(topic.getName()).size())
                      .filter(
                          partition ->
                              currentPartitions.get(topic.getName()).get(partition).size()
                                  != topic
                                      .getReplicationFactor()
                                      .orElseGet(this::getDefaultReplicationFactor))
                      .mapToObj(
                          partition ->
                              new Partition(
                                  partition,
                                  selectBrokersForReplication(
                                      currentPartitions.get(topic.getName()).get(partition),
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

  public Map<String, Map<String, Optional<String>>> topicConfigUpdate(
      Collection<ExistingTopic> currentState, Collection<Topic> requiredState) {
    Map<String, Map<String, Optional<String>>> result = new HashMap<>();

    Map<String, Map<String, String>> currentConfig =
        currentState.stream().collect(toMap(ExistingTopic::getName, ExistingTopic::getConfig));
    Map<String, Map<String, String>> requiredConfig =
        requiredState.stream().collect(toMap(Topic::getName, Topic::getConfig));

    requiredState.stream()
        .filter(topic -> alreadyExists(currentState, topic))
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

  public List<Integer> selectBrokersForReplication(
      List<Integer> currentState, int replicationFactor) {
    if (!allBrokers.stream()
        .map(Broker::getId)
        .collect(Collectors.toList())
        .containsAll(currentState)) {
      throw new RuntimeException(
          String.format(
              "Invalid replication state - All Brokers: %s, Used Brokers: %s",
              allBrokers.toString(), currentState.toString()));
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

    if (replicationFactor > currentState.size()) {
      List<Integer> availableBrokers =
          allBrokers.stream().map(Broker::getId).collect(Collectors.toList());
      availableBrokers.removeAll(currentState);
      Collections.shuffle(availableBrokers);
      List<Integer> result = availableBrokers.subList(0, replicationFactor - currentState.size());
      result.addAll(currentState);
      return result;
    } else {
      List<Integer> currentStateMutable = new ArrayList<>(currentState);
      Collections.shuffle(currentStateMutable);
      return currentStateMutable.subList(0, replicationFactor);
    }
  }

  public Map<String, Map<String, Optional<String>>> brokerConfigUpdate(
      Map<String, String> requiredState) {
    Map<String, Map<String, Optional<String>>> result = new HashMap<>();

    allBrokers.forEach(
        broker -> {
          Map<String, Optional<String>> configToApply = new HashMap<>();

          /*
           * Reset config value to the default if the config has been removed from yaml file
           */
          broker.getConfig().values().stream()
              .filter(config -> !requiredState.containsKey(config.getName()))
              .filter(config -> !config.isDefault())
              .filter(config -> !config.isReadOnly())
              .forEach(config -> configToApply.put(config.getName(), Optional.empty()));

          /*
           * Update config values
           */
          broker.getConfig().values().stream()
              .filter(config -> requiredState.containsKey(config.getName()))
              .filter(config -> !config.isDefault())
              .filter(config -> !requiredState.get(config.getName()).equals(config.getValue()))
              .forEach(
                  config ->
                      configToApply.put(
                          config.getName(),
                          Optional.ofNullable(requiredState.get(config.getName()))));

          result.put(String.valueOf(broker.getId()), configToApply);
        });

    return result;
  }

  private boolean alreadyExists(Collection<ExistingTopic> existingTopics, Topic topic) {
    return existingTopics.stream()
        .map(ExistingTopic::getName)
        .collect(toList())
        .contains(topic.getName());
  }

  private Integer getDefaultReplicationFactor() {
    return allBrokers.stream()
        .map(Broker::getConfig)
        .map(config -> config.get("default.replication.factor"))
        .filter(Objects::nonNull)
        .map(BrokerConfig::getValue)
        .map(Integer::valueOf)
        .max(Integer::compareTo)
        .orElse(1);
  }

  private Integer getDefaultPartitionCount() {
    return allBrokers.stream()
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
    String topicName;
    Map<String, String> oldConfig;
    Map<String, String> newConfig;
  }
}
