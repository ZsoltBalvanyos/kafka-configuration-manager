package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static java.util.stream.Collectors.*;

import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DeltaCalculator {

    public Set<Model.Topic> topicsToCreate(Collection<Model.ExistingTopic> currentState, Collection<Model.Topic> requiredState) {
        return requiredState
            .stream()
            .filter(topic -> !currentState
                .stream()
                .map(Model.ExistingTopic::getName)
                .collect(toSet())
                .contains(topic.getName()))
            .collect(toSet());
    }

    public Set<Model.ExistingTopic> topicsToDelete(Collection<Model.ExistingTopic> currentState, Collection<Model.Topic> requiredState) {
        return currentState
            .stream()
            .filter(topic -> !requiredState
                .stream()
                .map(Model.Topic::getName)
                .collect(toSet())
                .contains(topic.getName()))
            .collect(toSet());
    }

    public Map<String, Optional<Integer>> partitionUpdate(Collection<Model.ExistingTopic> currentState, Collection<Model.Topic> requiredState) {
        Map<String, Set<Model.Partition>> currentStateMap = currentState.stream().collect(toMap(Model.ExistingTopic::getName, Model.ExistingTopic::getPartitions));

        Map<String, Optional<Integer>> result = new HashMap<>();

        requiredState
            .stream()
            .filter(topic -> alreadyExists(currentState, topic))
            .forEach(topic ->
                topic.getPartitionCount().ifPresent(partitionCount -> {
                    if (partitionCount < currentStateMap.get(topic.getName()).size()) {
                        throw new RuntimeException(
                            String.format("Number of partitions cannot be lowered. Current number of partitions: %d, requested number of partitions: %d",
                                currentStateMap.get(topic.getName()).size(),
                                partitionCount));
                    }
                    if (partitionCount > currentStateMap.get(topic.getName()).size()) {
                        result.put(topic.getName(), topic.getPartitionCount());
                    }
                })
            );

        return result;
    }

    public Map<String, Collection<Model.Partition>> replicationUpdate(Set<Model.ExistingTopic> currentState, Set<Model.Topic> requiredState, Set<Model.Broker> allBrokers) {
        Map<String, Collection<Model.Partition>> result = new HashMap<>();

        Map<String, Map<Integer, List<Model.Broker>>> currentPartitions = currentState
            .stream()
            .collect(toMap(
                Model.ExistingTopic::getName,
                e -> e.getPartitions().stream().collect(toMap(Model.Partition::getPartitionNumber, Model.Partition::getReplicas))));

        requiredState
            .stream()
            .filter(topic -> alreadyExists(currentState, topic))
            .forEach(topic -> {
                Set<Model.Partition> partitions = IntStream
                    .range(0, currentPartitions.get(topic.getName()).size())
                    .filter(partition -> currentPartitions.get(topic.getName()).get(partition).size() != topic.getReplicationFactor())
                    .mapToObj(partition -> new Model.Partition(
                        partition,
                        selectBrokersForReplication(
                            allBrokers,
                            currentPartitions.get(topic.getName()).get(partition),
                            topic.getReplicationFactor())))
                    .collect(Collectors.toSet());

                if (!partitions.isEmpty()) {
                    result.put(topic.getName(), partitions);
                }
            });

        return result;
    }

    protected List<Model.Broker> selectBrokersForReplication(Collection<Model.Broker> allBrokers, List<Model.Broker> currentState, int replicationFactor) {
        if (!allBrokers.containsAll(currentState)) {
            throw new RuntimeException(String.format("Invalid replication state - All Brokers: %s, Used Brokers: %s", allBrokers.toString(), currentState.toString()));
        }
        if (replicationFactor < 1) {
            throw new RuntimeException(String.format("Replication factor must be greater than 0"));
        }
        if (replicationFactor > allBrokers.size()) {
            throw new RuntimeException(String.format("Replication factor [%d] must not be greater than the number of brokers [%d]", replicationFactor, allBrokers.size()));
        }

        if (replicationFactor > currentState.size()) {
            List<Model.Broker> availableBrokers = new ArrayList<>(allBrokers);
            availableBrokers.removeAll(currentState);
            Collections.shuffle(availableBrokers);
            List<Model.Broker> result = availableBrokers.subList(0, replicationFactor - currentState.size());
            result.addAll(currentState);
            return result;
        } else {
            List<Model.Broker> currentStateMutable = new ArrayList<>(currentState);
            Collections.shuffle(currentStateMutable);
            return currentStateMutable.subList(0, replicationFactor);
        }
    }

    public Map<String, Map<String, Optional<String>>> topicConfigUpdate(Collection<Model.ExistingTopic> currentState, Collection<Model.Topic> requiredState) {
        Map<String, Map<String, Optional<String>>> result = new HashMap<>();

        Map<String, Map<String, String>> currentConfig = currentState.stream().collect(toMap(Model.ExistingTopic::getName, Model.ExistingTopic::getConfig));
        Map<String, Map<String, String>> requiredConfig = requiredState.stream().collect(toMap(Model.Topic::getName, Model.Topic::getConfig));

        requiredState
            .stream()
            .filter(topic -> alreadyExists(currentState, topic))
            .map(topic -> new ConfigUpdate(topic.getName(), currentConfig.get(topic.getName()), requiredConfig.get(topic.getName())))
            .forEach(configUpdate -> {
                Set<String> configNames = new HashSet<>();
                configNames.addAll(configUpdate.oldConfig.keySet());
                configNames.addAll(configUpdate.newConfig.keySet());

                Map<String, Optional<String>> configToApply = new HashMap<>();
                configNames.forEach(configName -> {
                    if (configUpdate.oldConfig.containsKey(configName) && !configUpdate.newConfig.containsKey(configName)) {
                        configToApply.put(configName, Optional.empty());
                    } else if (!configUpdate.oldConfig.getOrDefault(configName, UUID.randomUUID().toString()).equals(configUpdate.newConfig.get(configName))) {
                        configToApply.put(configName, Optional.ofNullable(configUpdate.newConfig.get(configName)));
                    }
                });
                if (!configToApply.isEmpty()) {
                    result.put(configUpdate.topicName, configToApply);
                }
            });

        return result;
    }

    private boolean alreadyExists(Collection<Model.ExistingTopic> existingTopics, Model.Topic topic) {
        return existingTopics
            .stream()
            .map(Model.ExistingTopic::getName)
            .collect(toSet())
            .contains(topic.getName());
    }

    @Value
    private static class ConfigUpdate {
        String topicName;
        Map<String, String> oldConfig;
        Map<String, String> newConfig;
    }
}
