package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static java.util.stream.Collectors.*;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

@Slf4j
@AllArgsConstructor
public class KafkaClient {

  private final Admin admin;
  private final int ongoingAssignmentRetries;

  @SneakyThrows
  public Set<Model.Broker> getAllBrokers() {
    Set<ConfigResource> configResources =
        admin.describeCluster().nodes().get().stream()
            .map(node -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())))
            .collect(toSet());

    Set<Model.Broker> result = new HashSet<>();
    admin
        .describeConfigs(configResources)
        .all()
        .get()
        .forEach(
            (resource, config) -> {
              Map<String, Model.BrokerConfig> brokerConfig = new HashMap<>();
              config
                  .entries()
                  .forEach(
                      c ->
                          brokerConfig.put(
                              c.name(),
                              new Model.BrokerConfig(
                                  c.name(), c.value(), c.isDefault(), c.isReadOnly())));
              result.add(new Model.Broker(Integer.parseInt(resource.name()), brokerConfig));
            });

    return result;
  }

  @SneakyThrows
  public void updateConfigOfBrokers(Map<String, Map<String, Optional<String>>> updates) {
    Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest =
        getAlterConfigRequest(updates, ConfigResource.Type.BROKER);
    log.info("Modifying broker configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  private Model.Acl toAcl(Map.Entry<ResourcePattern, List<AclBinding>> resourceToAcl) {
    return new Model.Acl(
        resourceToAcl.getKey().resourceType().name(),
        resourceToAcl.getKey().name(),
        resourceToAcl.getKey().patternType().name(),
        resourceToAcl.getValue().stream()
            .map(
                aclBinding ->
                    new Model.Permission(
                        aclBinding.entry().principal(),
                        aclBinding.entry().host(),
                        aclBinding.entry().operation().name(),
                        aclBinding.entry().permissionType().name()))
            .collect(toSet()));
  }

  @SneakyThrows
  protected Set<Model.Acl> getAcls() {
    try {
      return admin
          .describeAcls(
              new AclBindingFilter(
                  new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
                  new AccessControlEntryFilter(
                      null, null, AclOperation.ANY, AclPermissionType.ANY)))
          .values()
          .get()
          .stream()
          .collect(groupingBy(AclBinding::pattern))
          .entrySet()
          .stream()
          .map(this::toAcl)
          .collect(toSet());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SecurityDisabledException) {
        return Set.of();
      } else {
        throw e;
      }
    }
  }

  @SneakyThrows
  protected void createAcls(Collection<Model.Acl> acls) {
    Set<AclBinding> aclBindings = acls.stream().flatMap(this::toAclBinding).collect(toSet());
    admin.createAcls(aclBindings).all().get();
  }

  public Stream<AclBinding> toAclBinding(Model.Acl acl) {
    return acl.getPermissions().stream()
        .map(
            permission ->
                new AclBinding(
                    new ResourcePattern(
                        ResourceType.valueOf(acl.getResourceType()),
                        acl.getName(),
                        PatternType.fromString(acl.getPatternType())),
                    new AccessControlEntry(
                        permission.getPrincipal(),
                        permission.getHost(),
                        AclOperation.fromString(permission.getOperation()),
                        AclPermissionType.fromString(permission.getPermissionType()))));
  }

  @SneakyThrows
  public void deleteAcls(Collection<Model.Acl> acls) {
    Set<AclBindingFilter> aclBindingFilters =
        acls.stream().flatMap(this::toAclBindingFilter).collect(toSet());
    admin.deleteAcls(aclBindingFilters).all().get();
  }

  public Stream<AclBindingFilter> toAclBindingFilter(Model.Acl acl) {
    return acl.getPermissions().stream()
        .map(
            permission ->
                new AclBindingFilter(
                    new ResourcePatternFilter(
                        ResourceType.valueOf(acl.getResourceType()),
                        acl.getName(),
                        PatternType.valueOf(acl.getPatternType())),
                    new AccessControlEntryFilter(
                        permission.getPrincipal(),
                        permission.getHost(),
                        AclOperation.valueOf(permission.getOperation()),
                        AclPermissionType.valueOf(permission.getPermissionType()))));
  }

  @SneakyThrows
  private Map<String, List<Model.Partition>> getPartitions(Collection<String> topicNames) {
    Map<String, List<Model.Partition>> result = new HashMap<>();

    admin
        .describeTopics(topicNames)
        .all()
        .get()
        .forEach(
            (topicName, description) -> {
              List<Model.Partition> partitions =
                  description.partitions().stream()
                      .map(
                          partitionInfo ->
                              new Model.Partition(
                                  partitionInfo.partition(),
                                  partitionInfo.replicas().stream()
                                      .map(Node::id)
                                      .collect(toList())))
                      .collect(toList());
              result.put(topicName, partitions);
            });
    return result;
  }

  @SneakyThrows
  private Map<String, Map<String, String>> getConfigs(Collection<TopicListing> existingTopics) {
    Map<String, Map<String, String>> result = new HashMap<>();
    admin
        .describeConfigs(
            existingTopics.stream()
                .map(
                    topicListing ->
                        new ConfigResource(ConfigResource.Type.TOPIC, topicListing.name()))
                .collect(toSet()))
        .all()
        .get()
        .forEach(
            (configResource, config) ->
                result.put(
                    configResource.name(),
                    config.entries().stream()
                        .filter(entry -> !entry.isDefault())
                        .collect(toMap(ConfigEntry::name, ConfigEntry::value))));
    return result;
  }

  @SneakyThrows
  public List<Model.ExistingTopic> getExistingTopics() {

    Collection<TopicListing> existingTopics = admin.listTopics().listings().get();

    Map<String, Map<String, String>> topicNameToConfig = getConfigs(existingTopics);
    Map<String, List<Model.Partition>> partitionMap =
        getPartitions(existingTopics.stream().map(TopicListing::name).collect(toList()));

    return existingTopics.stream()
        .map(
            topic ->
                new Model.ExistingTopic(
                    topic.name(),
                    Optional.ofNullable(partitionMap.get(topic.name()))
                        .orElseThrow(
                            () ->
                                new RuntimeException(
                                    "Not found partition info of topic " + topic.name())),
                    Optional.ofNullable(topicNameToConfig.get(topic.name()))
                        .orElseThrow(
                            () ->
                                new RuntimeException(
                                    "Not found configuration of topic " + topic.name()))))
        .collect(toList());
  }

  @SneakyThrows
  public void createTopics(Collection<Model.Topic> topics) {
    List<NewTopic> newTopics = topics.stream().map(this::toNewTopic).collect(toList());
    log.debug("Creating topics: {}", newTopics);
    admin.createTopics(newTopics).all().get();
  }

  public NewTopic toNewTopic(Model.Topic topic) {
    return new NewTopic(
            topic.getName(),
            topic.getPartitionCount(),
            topic.getReplicationFactor().map(Integer::shortValue))
        .configs(topic.getConfig());
  }

  @SneakyThrows
  public void deleteTopics(Collection<String> topics) {
    log.debug("Deleting topics: {}", topics);
    admin.deleteTopics(topics).all().get();
  }

  @SneakyThrows
  public boolean noOngoingReassignment() {
    Map<TopicPartition, PartitionReassignment> ongoingAssignments;
    int retries = 0;
    do {
      ongoingAssignments = admin.listPartitionReassignments().reassignments().get();
      if (!ongoingAssignments.isEmpty()) {
        Thread.sleep(10L * (int) Math.pow(2, retries++));
      }
    } while (!ongoingAssignments.isEmpty() && retries < ongoingAssignmentRetries);

    return retries != ongoingAssignmentRetries;
  }

  @SneakyThrows
  public void updatePartitions(Map<String, Integer> updates) {
    Map<String, NewPartitions> newPartitions = new HashMap<>();
    updates.forEach(
        (topicName, partitions) ->
            newPartitions.put(topicName, NewPartitions.increaseTo(partitions)));
    log.debug("Increasing partition count: {}", newPartitions);
    admin.createPartitions(newPartitions).all().get();
  }

  @SneakyThrows
  public void updateReplication(Map<String, List<Model.Partition>> updates) {
    Map<TopicPartition, Optional<NewPartitionReassignment>> topicPartitions = new HashMap<>();
    updates.forEach(
        (topicName, partitions) ->
            partitions.forEach(
                partition ->
                    topicPartitions.put(
                        new TopicPartition(topicName, partition.getPartitionNumber()),
                        Optional.of(new NewPartitionReassignment(partition.getReplicas())))));
    log.debug("Modifying replication: {}", topicPartitions);
    admin.alterPartitionReassignments(topicPartitions).all().get();
  }

  @SneakyThrows
  public void updateConfigOfTopics(Map<String, Map<String, Optional<String>>> updates) {
    Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest =
        getAlterConfigRequest(updates, ConfigResource.Type.TOPIC);
    log.debug("Modifying topic configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  protected Map<ConfigResource, Collection<AlterConfigOp>> getAlterConfigRequest(
      Map<String, Map<String, Optional<String>>> updates, ConfigResource.Type type) {

    Map<ConfigResource, Collection<AlterConfigOp>> alterConfigRequest = new HashMap<>();

    updates.forEach(
        (name, requiredConfigs) -> {
          Set<AlterConfigOp> alterConfigOps = new HashSet<>();

          requiredConfigs.forEach(
              (key, value) ->
                  alterConfigOps.add(
                      new AlterConfigOp(
                          new ConfigEntry(key, value.orElse("")),
                          value
                              .map(v -> AlterConfigOp.OpType.SET)
                              .orElse(AlterConfigOp.OpType.DELETE))));

          alterConfigRequest.put(new ConfigResource(type, name), alterConfigOps);
        });
    return alterConfigRequest;
  }

  public void close() {
    admin.close();
  }
}
