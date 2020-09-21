package com.zsoltbalvanyos.kafkaconfigurationmanager;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

@AllArgsConstructor
public class KafkaClient {

    final Logger log = LoggerFactory.getLogger(getClass().getName());
    private final Admin admin;

    public Set<Model.Broker> getAllBrokers() throws ExecutionException, InterruptedException {
        return admin
            .describeCluster()
            .nodes()
            .get()
            .stream()
            .map(node -> new Model.Broker(node.id()))
            .collect(Collectors.toSet());
    }

    protected Collection<AclBinding> getAcls() throws ExecutionException, InterruptedException {
        return admin.describeAcls(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY))
        ).values().get();
    }

    protected Map<String, Set<Model.Partition>> getPartitions(Collection<String> topicNames) throws ExecutionException, InterruptedException {
        Map<String, Set<Model.Partition>> result = new HashMap<>();

        DescribeTopicsResult d = admin.describeTopics(topicNames);

        KafkaFuture<Map<String, TopicDescription>> x = admin
            .describeTopics(topicNames)
            .all();

        admin
            .describeTopics(topicNames)
            .all()
            .get()
            .forEach((topicName, description) -> {
                Set<Model.Partition> partitions = description
                    .partitions()
                    .stream()
                    .map(partitionInfo -> new Model.Partition(
                        partitionInfo.partition(),
                        partitionInfo
                            .replicas()
                            .stream()
                            .map(Node::id)
                            .map(Model.Broker::new)
                            .collect(Collectors.toList())))
                    .collect(Collectors.toSet());
                result.put(topicName, partitions);
            });
        return result;
    }

    protected Map<String, Map<String, String>> getConfigs(Collection<TopicListing> existingTopics) throws ExecutionException, InterruptedException {
        Map<String, Map<String, String>> result = new HashMap<>();
        admin
            .describeConfigs(existingTopics
                .stream()
                .map(topicListing -> new ConfigResource(ConfigResource.Type.TOPIC, topicListing.name()))
                .collect(Collectors.toSet()))
            .all()
            .get()
            .forEach((configResource, config) -> result.put(
                configResource.name(),
                config.entries().stream().filter(entry -> !entry.isDefault()).collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))));
        return result;
    }

    public Set<Model.ExistingTopic> getExistingTopics() throws ExecutionException, InterruptedException {

        Collection<TopicListing> existingTopics =
            admin
                .listTopics()
                .listings()
                .get();

        Map<String, Map<String, String>> topicNameToConfig = getConfigs(existingTopics);
        Map<String, Set<Model.Partition>> partitionMap = getPartitions(existingTopics.stream().map(TopicListing::name).collect(Collectors.toSet()));

        return existingTopics
            .stream()
            .map(topic -> new Model.ExistingTopic(
                topic.name(),
                Optional.ofNullable(partitionMap.get(topic.name())).orElseThrow(() -> new RuntimeException("Not found partition info of topic " + topic.name())),
                Optional.ofNullable(topicNameToConfig.get(topic.name())).orElseThrow(() -> new RuntimeException("Not found configuration of topic " + topic.name()))))
            .collect(Collectors.toSet());
    }

    public void createTopics(Collection<Model.Topic> topics) throws ExecutionException, InterruptedException {
        Set<NewTopic> newTopics = topics
            .stream()
            .map(topic -> new NewTopic(topic.getName(), topic.getPartitionCount(), (short) topic.getReplicationFactor()).configs(topic.getConfig()))
            .collect(Collectors.toSet());

        log.info("Creating topics: {}", newTopics);

        admin.createTopics(newTopics).all().get();
    }

    public void deleteTopics(Collection<Model.ExistingTopic> topics) throws ExecutionException, InterruptedException {
        Set<String> topicsToDelete = topics.stream().map(Model.ExistingTopic::getName).collect(Collectors.toSet());
        log.info("Deleting topics: {}", topics);
        admin.deleteTopics(topicsToDelete).all().get();
    }

    public boolean noOngoingReassignment() throws ExecutionException, InterruptedException {
        Map<TopicPartition, PartitionReassignment> ongoingAssignments;
        int retries = 0;
        do {
            ongoingAssignments = admin.listPartitionReassignments().reassignments().get();
            if (!ongoingAssignments.isEmpty()) {
                Thread.sleep(100 * ++retries);
            }
        } while (!ongoingAssignments.isEmpty() && retries < 10);

        return true;
    }

    public void updatePartitions(Map<String, Integer> updates) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        updates.forEach((topicName, partitions) -> newPartitions.put(topicName, NewPartitions.increaseTo(partitions)));
        log.info("Increasing partition count: {}", newPartitions);
        admin.createPartitions(newPartitions).all().get();
    }

    public void updateReplication(Map<String, Collection<Model.Partition>> updates) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Optional<NewPartitionReassignment>> topicPartitions = new HashMap<>();
        updates.forEach((topicName, partitions) ->
            partitions.forEach(partition -> topicPartitions.put(
                new TopicPartition(topicName, partition.getPartitionNumber()),
                Optional.of(new NewPartitionReassignment(partition.getReplicas().stream().map(Model.Broker::getId).collect(Collectors.toList())))))
        );
        log.info("Modifying replication: {}", topicPartitions);
        admin.alterPartitionReassignments(topicPartitions).all().get();
    }

    public void updateConfigOfTopics(Map<String, Map<String, Optional<String>>> updates) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest = getAlterConfigRequest(updates);
        log.info("Modifying topic configurations: {}", alterConfigRequest);
        admin.incrementalAlterConfigs(alterConfigRequest).all().get();
    }

    protected Map<ConfigResource, Collection<AlterConfigOp>> getAlterConfigRequest(Map<String, Map<String, Optional<String>>> updates) {

        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest = new HashMap<>();

        updates.forEach((topicName, requiredConfigs) -> {

            Set<AlterConfigOp> alterConfigOps = new HashSet<>();

            requiredConfigs.forEach((name, config) ->
                alterConfigOps.add(
                    new AlterConfigOp(
                        new ConfigEntry(name, config.orElse("")),
                        config.map(v -> AlterConfigOp.OpType.SET).orElse(AlterConfigOp.OpType.DELETE)
                    ))
            );

            alterConfigRequest.put(
                new ConfigResource(ConfigResource.Type.TOPIC, topicName),
                alterConfigOps
            );
        });
        return alterConfigRequest;
    }

}
