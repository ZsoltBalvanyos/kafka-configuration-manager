package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.*;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import io.vavr.collection.List;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import io.vavr.control.Option;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import lombok.RequiredArgsConstructor;
import lombok.Value;

@RequiredArgsConstructor
public class DeltaCalculator {

  final CurrentState currentState;
  final RequiredState requiredState;

  public Traversable<Acl> aclsToCreate() {
    return requiredState.getAcls().filter(acl -> !currentState.getAcls().contains(acl));
  }

  public Traversable<Acl> aclsToDelete() {
    return currentState.getAcls().filter(acl -> !requiredState.getAcls().contains(acl));
  }

  public Traversable<RequiredTopic> topicsToCreate() {
    return requiredState
        .getTopics()
        .filter(topic -> notContains(currentState.getTopics(), topic.getName()));
  }

  public Traversable<ExistingTopic> topicsToDelete() {
    return currentState
        .getTopics()
        .filter(topic -> notContains(requiredState.getTopics(), topic.getName()));
  }

  private boolean notContains(Traversable<? extends Identified> topics, Id<?> topicName) {
    return !topics.map(Identified::getName).collect(toList()).contains(topicName);
  }

  public Map<TopicName, Integer> partitionUpdate() {
    var topicToPartitionMap =
        currentState.getTopics().toMap(ExistingTopic::getName, ExistingTopic::getPartitions);
    return requiredState
        .getTopics()
        .filter(this::exists)
        .toMap(RequiredTopic::getName, RequiredTopic::getPartitionCount)
        .mapValues(pc -> pc.orElseGet(this::getDefaultPartitionCount))
        .filter(
            (topic, reqPartCount) ->
                validPartitionCount(topicToPartitionMap.get(topic).get().size(), reqPartCount));
  }

  private boolean validPartitionCount(int current, int required) {
    if (required < current) {
      throw new RuntimeException(
          String.format(
              "Number of partitions cannot be lowered. Current number of partitions: %d, requested number of partitions: %d",
              current, required));
    }
    return required > current;
  }

  public Map<TopicName, Traversable<Partition>> replicationUpdate() {
    var topicToPartitionMap =
        currentState.getTopics().toMap(ExistingTopic::getName, ExistingTopic::getPartitions);

    return requiredState
        .getTopics()
        .filter(this::exists)
        .toMap(RequiredTopic::getName, RequiredTopic::getReplicationFactor)
        .mapValues(rep -> rep.orElseGet(this::getDefaultReplicationFactor))
        .map(
            (topic, requiredRep) ->
                Map.entry(
                    topic,
                    topicToPartitionMap
                        .get(topic)
                        .get()
                        .filter((partitionNumber, replicas) -> replicas.size() != requiredRep)
                        .mapValues(replicas -> selectBrokersForReplication(replicas, requiredRep))
                        .map(p -> new Partition(p._1, p._2))));
  }

  public Map<TopicName, Map<String, Optional<String>>> topicConfigUpdate() {

    Map<TopicName, Map<String, String>> currentConfig =
        currentState.getTopics().toMap(ExistingTopic::getName, ExistingTopic::getConfig);

    Map<TopicName, Map<String, String>> requiredConfig =
        requiredState.getTopics().toMap(RequiredTopic::getName, RequiredTopic::getConfig);

    Function<RequiredTopic, ConfigUpdate> toConfigUpdate =
        topic ->
            new ConfigUpdate(
                topic.getName(),
                currentConfig.get(topic.getName()).get(),
                requiredConfig.get(topic.getName()).get());

    return requiredState
        .getTopics()
        .filter(this::exists)
        .map(toConfigUpdate)
        .toMap(ConfigUpdate::getTopicName, Function.identity())
        .mapValues(this::getKeyValueMap);
  }

  private Map<String, Optional<String>> getKeyValueMap(ConfigUpdate configUpdate) {
    var oldConfig = configUpdate.oldConfig;
    var newConfig = configUpdate.newConfig;
    List<String> configNames = List.ofAll(oldConfig.keySet()).appendAll(newConfig.keySet());

    Predicate<String> removed =
        configName -> oldConfig.containsKey(configName) && !newConfig.containsKey(configName);

    Predicate<String> changed =
        configName ->
            !oldConfig
                .get(configName)
                .getOrElse(UUID.randomUUID().toString())
                .equals(newConfig.get(configName).getOrElse(UUID.randomUUID().toString()));

    return configNames
        .filter(removed.or(changed))
        .toMap(
            configName ->
                removed.test(configName)
                    ? Map.entry(configName, Optional.empty())
                    : Map.entry(configName, newConfig.get(configName).toJavaOptional()));
  }

  public Map<BrokerId, Map<String, Optional<String>>> brokerConfigUpdate() {

    Map<BrokerId, Map<String, BrokerConfig>> brokerIdMapMap =
        currentState.getBrokers().toMap(Broker::getId, Broker::getConfig);

    return brokerIdMapMap.mapValues(this::updateConfig);
  }

  private Map<String, Optional<String>> updateConfig(Map<String, BrokerConfig> configs) {
    var requiredBrokerConfig = requiredState.getBrokers();
    Predicate<BrokerConfig> removed = config -> !requiredBrokerConfig.containsKey(config.getName());
    Predicate<BrokerConfig> changed =
        config ->
            !requiredBrokerConfig
                .get(config.getName())
                .map(config.getValue()::equals)
                .getOrElse(false);

    var result =
        configs
            .values()
            .filter(removed.or(changed))
            .filter(not(BrokerConfig::isDefault))
            .filter(not(BrokerConfig::isReadOnly))
            .toMap(
                config ->
                    removed.test(config)
                        ? Map.entry(config.getName(), Optional.<String>empty())
                        : Map.entry(
                            config.getName(),
                            requiredBrokerConfig.get(config.getName()).toJavaOptional()));

    System.out.println("--> " + result);
    return result;
  }

  public List<Integer> selectBrokersForReplication(
      Traversable<Integer> utilizedBrokers, int replicationFactor) {

    var allBrokers =
        currentState.getBrokers().map(Broker::getId).map(Id::get).map(Integer::valueOf);

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
      return List.ofAll(allBrokers)
          .removeAll(utilizedBrokers)
          .shuffle()
          .subSequence(0, replicationFactor - utilizedBrokers.size())
          .appendAll(utilizedBrokers);
    } else {
      return List.ofAll(utilizedBrokers).shuffle().subSequence(0, replicationFactor);
    }
  }

  private boolean exists(RequiredTopic requiredTopic) {
    return currentState.getTopics().map(ExistingTopic::getName).contains(requiredTopic.getName());
  }

  private Integer getDefaultReplicationFactor() {
    return getDefaultInteger("default.replication.factor");
  }

  private Integer getDefaultPartitionCount() {
    return getDefaultInteger("num.partitions");
  }

  private Integer getDefaultInteger(String configName) {
    return currentState
        .getBrokers()
        .map(Broker::getConfig)
        .map(config -> config.get(configName))
        .filter(Option::isDefined)
        .map(Option::get)
        .map(BrokerConfig::getValue)
        .map(Integer::valueOf)
        .max()
        .getOrElse(1);
  }

  @Value
  private static class ConfigUpdate {
    TopicName topicName;
    Map<String, String> oldConfig;
    Map<String, String> newConfig;
  }
}
