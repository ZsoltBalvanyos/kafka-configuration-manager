package com.zsoltbalvanyos.kafkaconfigurationmanager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Reporter {

    Logger log = LoggerFactory.getLogger(getClass().getName());

    final static private String NEWLINE = "\n";
    final static private String INDENTED_NEWLINE = "\n\t\t";

    public void printChanges(
        Map<String, Collection<Model.Partition>> replicationChanges,
        Map<String, Integer> partitionChanges,
        Map<String, Map<String, Optional<String>>> topicConfigurationChanges,
        Set<Model.Topic> topicsToCreate,
        Set<Model.ExistingTopic> topicsToDelete
    ) {

        StringBuilder sb = new StringBuilder();
        sb.append(NEWLINE);

        if (!replicationChanges.isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in replication:")
                .append(NEWLINE);
            replicationChanges.forEach((topic, partition) -> {
                sb.append(topic);
                partition.forEach(p -> sb.append(INDENTED_NEWLINE).append(p));
                sb.append(NEWLINE);
            });
        }

        if (!partitionChanges.isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in partition count:")
                .append(NEWLINE);
            partitionChanges.forEach((topic, partCont) -> {
                sb.append(topic + " -> " + partCont).append(NEWLINE);
            });
        }

        if (!topicConfigurationChanges.isEmpty()) {
            sb.append(NEWLINE)
                .append("Changes in topic configuration:")
                .append(NEWLINE);
            topicConfigurationChanges.forEach((topic, partition) -> {
                sb.append(topic);
                partition.forEach((name, value) -> sb.append(INDENTED_NEWLINE).append(name + " -> " + value));
                sb.append(NEWLINE);
            });
        }

        if (!topicsToCreate.isEmpty()) {
            sb.append(NEWLINE)
                .append("New topics to create:")
                .append(NEWLINE);
            topicsToCreate.forEach(topic -> sb.append(topic).append(NEWLINE));
        }

        if (!topicsToDelete.isEmpty()) {
            sb.append(NEWLINE)
                .append("Existing topics to delete:")
                .append(NEWLINE);
            topicsToDelete.forEach(topic -> sb.append(topic).append(NEWLINE));
        }

        sb.append(NEWLINE);
        log.info(sb.toString());
    }
}
