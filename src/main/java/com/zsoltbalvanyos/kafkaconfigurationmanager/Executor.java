package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.ExecutionPlan;

import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@AllArgsConstructor
public class Executor {

    final Logger log = LoggerFactory.getLogger(getClass().getName());
    final KafkaClient kafkaClient;
    final DeltaCalculator deltaCalculator;
    final List<Callable<String>> rollbacks = new ArrayList<>();

    public void run(ExecutionPlan plan) throws Exception {

        try {
            if (!plan.getTopicConfigurationChanges().isEmpty()) {
                kafkaClient.updateConfigOfTopics(plan.getTopicConfigurationChanges());
                rollbacks.add(rollbackConfig(plan.getOriginalConfigs(), plan.getTopicConfigurationChanges()));
                log.info("Topic configurations have been updated successfully.");
            }

            if (!plan.getTopicsToCreate().isEmpty()) {
                kafkaClient.createTopics(plan.getTopicsToCreate());
                rollbacks.add(rollbackCreate(plan.getTopicsToCreate()));
                log.info("Topics have been created successfully.");
            }

            if (!plan.getReplicationChanges().isEmpty()) {
                kafkaClient.updateReplication(plan.getReplicationChanges());
                rollbacks.add(rollbackReplication(plan.getOriginalPartitions(), plan.getReplicationChanges()));
                log.info("Replication factors have been updated successfully.");
            }

            if (!plan.getPartitionChanges().isEmpty() && kafkaClient.noOngoingReassignment()) {
                kafkaClient.updatePartitions(plan.getPartitionChanges());
                log.info("Partition counts have been updated successfully.");
            }

            try {
                if (!plan.getTopicsToDelete().isEmpty()) {
                    kafkaClient.deleteTopics(plan.getTopicsToDelete().stream().map(Model.ExistingTopic::getName).collect(Collectors.toSet()));
                    log.info("Topics have been deleted successfully.");
                }
            } catch (Exception e) {
                log.error("Failed to delete topics due to {}", e.getMessage());
            }

        } catch (Exception e) {
            log.error("Rolling back changes due to: {}", e.getMessage());
            for(Callable<String> rollback: rollbacks) {
                log.warn(rollback.call());
            }
        }
    }

    public Callable<String> rollbackCreate(Set<Model.Topic> createdTopics) {
        return () -> {
            kafkaClient.deleteTopics(createdTopics.stream().map(Model.Topic::getName).collect(Collectors.toSet()));
            return "Rolled back new topic creation";
        };
    }

    public Callable<String> rollbackConfig(Map<String, Map<String, String>> originalConfigs,
                               Map<String, Map<String, Optional<String>>> configChanges) {

        Map<String, Map<String, Optional<String>>> rollback = new HashMap<>();

        configChanges.forEach((topicName, config) -> {
            Map<String, Optional<String>> topicConfig = new HashMap<>();
            originalConfigs.get(topicName).forEach((name, value) -> topicConfig.put(name, Optional.of(value)));
            rollback.put(topicName, topicConfig);
        });

        return () -> {
            kafkaClient.updateConfigOfTopics(rollback);
            return "Rolled back topic configuration";
        };

    }

    public Callable<String> rollbackReplication(Map<String, Map<Integer, Integer>> originalPartitions,
                                    Map<String, Collection<Model.Partition>> replicationChanges) {
        Map<String, Collection<Model.Partition>> rollback = new HashMap<>();

        replicationChanges.forEach((topicName, replicationChange) -> {

            Set<Model.Partition> rollbackTo = new HashSet<>();

            originalPartitions
                .get(topicName)
                .forEach((partition, replication) ->
                    rollbackTo.add(new Model.Partition(
                        partition,
                        deltaCalculator.selectBrokersForReplication(
                            replicationChange
                                .stream()
                                .map(Model.Partition::getPartitionNumber)
                                .collect(Collectors.toList()),
                            replication))));

            rollback.put(topicName, rollbackTo);
        });

        return () -> {
            kafkaClient.updateReplication(rollback);
            return "Rolled back replication changes";
        };
    }
}
