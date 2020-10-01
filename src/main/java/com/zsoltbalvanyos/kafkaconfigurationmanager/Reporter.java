package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import static java.util.stream.Collectors.*;

@RequiredArgsConstructor
public class Reporter {

    Logger log = LoggerFactory.getLogger(getClass().getName());

    final static private String NEWLINE = "\n";
    final static private String INDENTED_NEWLINE = "\n\t";
    final static private String DOUBLE_INDENTED_NEWLINE = "\n\t\t";

    public void print(Set<ExistingTopic> existingTopics) {
        StringBuilder sb = new StringBuilder()
            .append(NEWLINE)
            .append("Current state of existing topics:");

        existingTopics.stream().map(this::printTopic).forEach(sb::append);

        log.info(sb.toString());
    }

    public void print(ExecutionPlan plan) {
        StringBuilder sb = new StringBuilder();
        sb.append(NEWLINE);

        if (!plan.getReplicationChanges().isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in replication:")
                .append(NEWLINE);
            plan.getReplicationChanges().forEach((topicName, partition) -> {
                sb.append(NEWLINE);
                sb.append(topicName);
                partition.forEach(p -> sb
                    .append(INDENTED_NEWLINE)
                    .append("partition[")
                    .append(p.getPartitionNumber())
                    .append("]: ")
                    .append(plan.getOriginalPartitions().get(topicName).get(p.getPartitionNumber()))
                    .append(" -> ")
                    .append(p.getReplicas().size()));
                sb.append(NEWLINE);
            });
        }

        if (!plan.getPartitionChanges().isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in partition count:")
                .append(NEWLINE);
            plan.getPartitionChanges().forEach((topicName, partitionCount) -> sb
                .append(NEWLINE)
                .append(topicName)
                .append(": ")
                .append(plan.getOriginalPartitions().get(topicName).size())
                .append(" -> ")
                .append(partitionCount));
            sb.append(NEWLINE);
        }

        if (!plan.getTopicConfigurationChanges().isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in topic configuration:")
                .append(NEWLINE);
            plan.getTopicConfigurationChanges().forEach((topicName, partition) -> {
                sb.append(NEWLINE);
                sb.append(topicName);
                partition.forEach((name, value) -> sb
                    .append(INDENTED_NEWLINE)
                    .append(name)
                    .append(": ")
                    .append(plan.getOriginalConfigs().get(topicName).getOrDefault(name, "default"))
                    .append(" -> ")
                    .append(value.orElse("default")));
                sb.append(NEWLINE);
            });
        }

        if (!plan.getTopicsToCreate().isEmpty()) {
            sb.append(NEWLINE)
                .append("New topics to create:")
                .append(NEWLINE);
            plan.getTopicsToCreate().stream().map(this::printTopic).forEach(sb::append);
        }

        if (!plan.getTopicsToDelete().isEmpty()) {
            sb.append(NEWLINE)
                .append("Existing topics to delete:")
                .append(NEWLINE);
            plan.getTopicsToDelete().stream().map(this::printTopic).forEach(sb::append);
        }

        sb.append(NEWLINE);
        log.info(sb.toString());
    }

    private String printTopic(Topic topic) {
        return new StringBuilder()
            .append(NEWLINE)
            .append(topic.getName())
            .append(INDENTED_NEWLINE)
            .append("Partition count: ")
            .append(topic.getPartitionCount().map(String::valueOf).orElse("default"))
            .append(INDENTED_NEWLINE)
            .append("Replication factor: ")
            .append(topic.getReplicationFactor().map(String::valueOf).orElse("default"))
            .append(printMap(topic.getConfig(), INDENTED_NEWLINE))
            .append(NEWLINE)
            .toString();
    }

    private String printTopic(ExistingTopic topic) {
        StringBuilder sb = new StringBuilder()
            .append(NEWLINE)
            .append(topic.getName())
            .append(printMap(topic.getConfig(), INDENTED_NEWLINE))
            .append(INDENTED_NEWLINE)
            .append("Partitions: ");
        topic.getPartitions().forEach(partition -> {
            sb.append(DOUBLE_INDENTED_NEWLINE)
                .append(String.format("Id: %d, replicas: [%s]", partition.getPartitionNumber(), partition.getReplicas().stream().map(String::valueOf).collect(joining(","))));
        });
        return sb.append(NEWLINE).toString();
    }

    private <K, V> String printMap(Map<K, V> map, String separator) {
        StringBuilder sb = new StringBuilder();
        map.forEach((name, value) -> {
            sb.append(separator);
            sb.append(name + ": " + value);
        });
        return sb.toString();
    }

}
