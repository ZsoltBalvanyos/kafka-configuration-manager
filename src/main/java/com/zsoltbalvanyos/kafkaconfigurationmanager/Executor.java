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
            if (!plan.getAclsToDelete().isEmpty()) {
                //TODO rollback
                kafkaClient.deleteAcls(plan.getAclsToDelete());
                log.info("ACLs have been deleted successfully");
            }

            if (!plan.getAclsToCreate().isEmpty()) {
                //TODO rollback
                kafkaClient.createAcls(plan.getAclsToCreate());
                log.info("New ACLs have been added successfully");
            }

            if (!plan.getTopicConfigurationChanges().isEmpty()) {
                rollbacks.add(rollbackConfig(plan.getOriginalConfigs(), plan.getTopicConfigurationChanges()));
                kafkaClient.updateConfigOfTopics(plan.getTopicConfigurationChanges());
                log.info("Topic configurations have been updated successfully.");
            }

            if (!plan.getTopicsToCreate().isEmpty()) {
                rollbacks.add(rollbackCreate(plan.getTopicsToCreate()));
                kafkaClient.createTopics(plan.getTopicsToCreate());
                log.info("Topics have been created successfully.");
            }

            if (!plan.getReplicationChanges().isEmpty()) {
                rollbacks.add(rollbackReplication(plan.getOriginalPartitions(), plan.getReplicationChanges()));
                kafkaClient.updateReplication(plan.getReplicationChanges());
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
            log.error(Arrays.stream(e.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.joining("\n\t")));
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

            Set<Model.Partition> rollbackTo = replicationChange
                .stream()
                .map(partition -> new Model.Partition(
                    partition.getPartitionNumber(),
                    deltaCalculator.selectBrokersForReplication(partition.getReplicas(), originalPartitions.get(topicName).get(partition.getPartitionNumber()))))
                .collect(Collectors.toSet());

            rollback.put(topicName, rollbackTo);
        });

        return () -> {
            kafkaClient.updateReplication(rollback);
            return "Rolled back replication changes";
        };
    }
}
