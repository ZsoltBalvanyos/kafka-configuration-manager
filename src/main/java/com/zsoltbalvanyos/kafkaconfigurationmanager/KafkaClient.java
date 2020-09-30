package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static java.util.stream.Collectors.*;

import lombok.AllArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutionException;

@AllArgsConstructor
public class KafkaClient {

    final Logger log = LoggerFactory.getLogger(getClass().getName());
    private final Admin admin;

    public Set<Model.Broker> getAllBrokers() throws ExecutionException, InterruptedException {
        Set<ConfigResource> configResources = admin
            .describeCluster()
            .nodes()
            .get()
            .stream()
            .map(node -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())))
            .collect(toSet());

        Set<Model.Broker> result = new HashSet<>();
        admin
            .describeConfigs(configResources)
            .all()
            .get()
            .forEach((resource, config) -> {
                Map<String, String> brokerConfig = new HashMap<>();
                config.entries().forEach(c -> brokerConfig.put(c.name(), c.value()));
                result.add(new Model.Broker(Integer.parseInt(resource.name()), brokerConfig));
            });

        return result;
    }

    private Model.Acl toAcl(Map.Entry<ResourcePattern, List<AclBinding>> resourceToAcl) {
        return new Model.Acl(
            resourceToAcl.getKey().resourceType().name(),
            resourceToAcl.getKey().name(),
            resourceToAcl.getKey().patternType().name(),
            resourceToAcl.getValue().stream().map(aclBinding -> new Model.Permission(
                aclBinding.entry().principal(),
                aclBinding.entry().host(),
                aclBinding.entry().operation().name(),
                aclBinding.entry().permissionType().name()
            )).collect(toSet())
        );
    }

    protected Set<Model.Acl> getAcls() throws ExecutionException, InterruptedException {
        return admin.describeAcls(new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY)
        )).values()
            .get()
            .stream()
            .collect(groupingBy(AclBinding::pattern))
            .entrySet()
            .stream()
            .map(this::toAcl)
            .collect(toSet());
    }

    protected void createAcls(Collection<Model.Acl> acls) throws ExecutionException, InterruptedException {
        List<AclBinding> aclBindings = acls.stream().flatMap(acl ->
            acl.getPermissions().stream().map(permission ->
                new AclBinding(
                    new ResourcePattern(
                        ResourceType.valueOf(acl.getResourceType()),
                        acl.getName(),
                        PatternType.fromString(acl.getPatternType())),
                    new AccessControlEntry(
                        permission.getPrincipal(),
                        permission.getHost(),
                        AclOperation.fromString(permission.getOperation()),
                        AclPermissionType.fromString(permission.getPermissionType())))))
            .collect(toList());

        admin.createAcls(aclBindings).all().get();
    }

    protected void deleteAcls(Collection<Model.Acl> acls) throws ExecutionException, InterruptedException {
        List<AclBindingFilter> aclBindingFilters = acls.stream().flatMap(acl ->
            acl.getPermissions().stream().map(permission ->
                new AclBindingFilter(
                    new ResourcePatternFilter(
                        ResourceType.valueOf(acl.getResourceType()),
                        acl.getName(),
                        PatternType.valueOf(acl.getPatternType())),
                    new AccessControlEntryFilter(
                        permission.getPrincipal(),
                        permission.getHost(),
                        AclOperation.valueOf(permission.getOperation()),
                        AclPermissionType.valueOf(permission.getPermissionType())))))
            .collect(toList());

        admin.deleteAcls(aclBindingFilters).all().get();
    }

    protected Map<String, Set<Model.Partition>> getPartitions(Collection<String> topicNames) throws ExecutionException, InterruptedException {
        Map<String, Set<Model.Partition>> result = new HashMap<>();

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
                            .collect(toList())))
                    .collect(toSet());
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
                .collect(toSet()))
            .all()
            .get()
            .forEach((configResource, config) -> result.put(
                configResource.name(),
                config.entries().stream().filter(entry -> !entry.isDefault()).collect(toMap(ConfigEntry::name, ConfigEntry::value))));
        return result;
    }

    public Set<Model.ExistingTopic> getExistingTopics() throws ExecutionException, InterruptedException {

        Collection<TopicListing> existingTopics =
            admin
                .listTopics()
                .listings()
                .get();

        Map<String, Map<String, String>> topicNameToConfig = getConfigs(existingTopics);
        Map<String, Set<Model.Partition>> partitionMap = getPartitions(existingTopics.stream().map(TopicListing::name).collect(toSet()));

        return existingTopics
            .stream()
            .map(topic -> new Model.ExistingTopic(
                topic.name(),
                Optional.ofNullable(partitionMap.get(topic.name())).orElseThrow(() -> new RuntimeException("Not found partition info of topic " + topic.name())),
                Optional.ofNullable(topicNameToConfig.get(topic.name())).orElseThrow(() -> new RuntimeException("Not found configuration of topic " + topic.name()))))
            .collect(toSet());
    }

    public void createTopics(Collection<Model.Topic> topics) throws ExecutionException, InterruptedException {
        Set<NewTopic> newTopics = topics
            .stream()
            .map(topic -> new NewTopic(topic.getName(), topic.getPartitionCount(), topic.getReplicationFactor().map(Integer::shortValue)).configs(topic.getConfig()))
            .collect(toSet());

        log.debug("Creating topics: {}", newTopics);

        admin.createTopics(newTopics).all().get();
    }

    public void deleteTopics(Collection<String> topics) throws ExecutionException, InterruptedException {
        log.debug("Deleting topics: {}", topics);
        admin.deleteTopics(topics).all().get();
    }

    public boolean noOngoingReassignment() throws ExecutionException, InterruptedException {
        Map<TopicPartition, PartitionReassignment> ongoingAssignments;
        int retries = 0;
        do {
            ongoingAssignments = admin.listPartitionReassignments().reassignments().get();
            if (!ongoingAssignments.isEmpty()) {
                Thread.sleep(100 * (int) Math.pow(2, retries++));
            }
        } while (!ongoingAssignments.isEmpty() && retries < 100);

        return true;
    }

    public void updatePartitions(Map<String, Integer> updates) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitions = new HashMap<>();
        updates.forEach((topicName, partitions) -> newPartitions.put(topicName, NewPartitions.increaseTo(partitions)));
        log.debug("Increasing partition count: {}", newPartitions);
        admin.createPartitions(newPartitions).all().get();
    }

    public void updateReplication(Map<String, Collection<Model.Partition>> updates) throws ExecutionException, InterruptedException {
        Map<TopicPartition, Optional<NewPartitionReassignment>> topicPartitions = new HashMap<>();
        updates.forEach((topicName, partitions) ->
            partitions.forEach(partition -> topicPartitions.put(
                new TopicPartition(topicName, partition.getPartitionNumber()),
                Optional.of(new NewPartitionReassignment(partition.getReplicas()))))
        );
        log.debug("Modifying replication: {}", topicPartitions);
        admin.alterPartitionReassignments(topicPartitions).all().get();
    }

    public void updateConfigOfTopics(Map<String, Map<String, Optional<String>>> updates) throws ExecutionException, InterruptedException {
        Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest = getAlterConfigRequest(updates);
        log.debug("Modifying topic configurations: {}", alterConfigRequest);
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

    public void close() {
        admin.close();
    }

}
