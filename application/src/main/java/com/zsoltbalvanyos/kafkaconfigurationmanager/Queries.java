package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.stream.Collectors.*;

import io.vavr.Tuple;
import io.vavr.collection.Stream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
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

@RequiredArgsConstructor
public class Queries {

  private final Admin admin;

  @SneakyThrows
  public Set<Acl> getAcls() {
    AclBindingFilter aclBindingFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    try {
      return admin.describeAcls(aclBindingFilter).values().get().stream()
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
  public Set<Broker> getBrokers() {
    Set<ConfigResource> configResources =
        admin.describeCluster().nodes().get().stream()
            .map(node -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())))
            .collect(toSet());

    Set<Broker> result = new HashSet<>();

    BiConsumer<ConfigResource, Config> populate =
        (resource, config) -> {
          var brokerConfig =
              Stream.ofAll(config.entries())
                  .map(c -> new BrokerConfig(c.name(), c.value(), c.isDefault(), c.isReadOnly()))
                  .toJavaMap(bc -> Tuple.of(bc.getName(), bc));
          result.add(new Broker(BrokerId.of(resource.name()), brokerConfig));
        };

    admin.describeConfigs(configResources).all().get().forEach(populate);

    return result;
  }

  @SneakyThrows
  public List<ExistingTopic> getExistingTopics() {

    Collection<TopicListing> existingTopics = admin.listTopics().listings().get();

    Map<String, Map<String, String>> topicNameToConfig = getTopicConfigs(existingTopics);
    Map<String, Map<PartitionNumber, Collection<Integer>>> partitionMap =
        getPartitions(existingTopics.stream().map(TopicListing::name).collect(toList()));

    Function<TopicListing, ExistingTopic> toExistingTopic =
        (topic) ->
            new ExistingTopic(
                TopicName.of(topic.name()),
                Optional.ofNullable(partitionMap.get(topic.name()))
                    .orElseThrow(
                        () ->
                            new RuntimeException(
                                "Not found partition info of topic " + topic.name())),
                Optional.ofNullable(topicNameToConfig.get(topic.name()))
                    .orElseThrow(
                        () ->
                            new RuntimeException(
                                "Not found configuration of topic " + topic.name())));

    return existingTopics.stream().map(toExistingTopic).collect(toList());
  }

  @SneakyThrows
  private Map<String, Map<PartitionNumber, Collection<Integer>>> getPartitions(
      Collection<String> topicNames) {
    Map<String, Map<PartitionNumber, Collection<Integer>>> result = new HashMap<>();

    BiConsumer<String, TopicDescription> populate =
        (topicName, description) ->
            result.put(
                topicName,
                description.partitions().stream()
                    .collect(
                        toMap(
                            p -> PartitionNumber.of(p.partition()),
                            p -> p.replicas().stream().map(Node::id).collect(toList()))));

    admin.describeTopics(topicNames).all().get().forEach(populate);

    return result;
  }

  @SneakyThrows
  private Map<String, Map<String, String>> getTopicConfigs(
      Collection<TopicListing> existingTopics) {
    Map<String, Map<String, String>> result = new HashMap<>();

    BiConsumer<ConfigResource, Config> populate =
        (configResource, config) ->
            result.put(
                configResource.name(),
                config.entries().stream()
                    .filter(entry -> !entry.isDefault())
                    .collect(toMap(ConfigEntry::name, ConfigEntry::value)));

    var configResources =
        existingTopics.stream()
            .map(topicListing -> new ConfigResource(ConfigResource.Type.TOPIC, topicListing.name()))
            .collect(toSet());

    admin.describeConfigs(configResources).all().get().forEach(populate);

    return result;
  }

  @SneakyThrows
  public boolean ongoingReassignment(int ongoingAssignmentRetries) {
    Map<TopicPartition, PartitionReassignment> ongoingAssignments;
    int retries = 0;
    do {
      ongoingAssignments = admin.listPartitionReassignments().reassignments().get();
      if (!ongoingAssignments.isEmpty()) {
        Thread.sleep(10L * (int) Math.pow(2, retries++));
      }
    } while (!ongoingAssignments.isEmpty() && retries < ongoingAssignmentRetries);

    return retries == ongoingAssignmentRetries;
  }

  public Model.Acl toAcl(Map.Entry<ResourcePattern, List<AclBinding>> resourceToAcl) {
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
}
