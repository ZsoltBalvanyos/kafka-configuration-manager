package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.*;

import io.vavr.Tuple;
import io.vavr.Tuple2;
import io.vavr.collection.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
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
  public Traversable<Acl> getAcls() {
    AclBindingFilter aclBindingFilter =
        new AclBindingFilter(
            new ResourcePatternFilter(ResourceType.ANY, null, PatternType.ANY),
            new AccessControlEntryFilter(null, null, AclOperation.ANY, AclPermissionType.ANY));

    try {
      return List.ofAll(admin.describeAcls(aclBindingFilter).values().get())
          .groupBy(AclBinding::pattern)
          .map(this::toAcl);
    } catch (ExecutionException e) {
      if (e.getCause() instanceof SecurityDisabledException) {
        return List.empty();
      } else {
        throw e;
      }
    }
  }

  @SneakyThrows
  public Traversable<Model.Broker> getBrokers() {
    return HashMap.ofAll(admin.describeConfigs(getConfigResources().collect(toSet())).all().get())
        .map(this::toBroker);
  }

  public Model.Broker toBroker(Tuple2<ConfigResource, Config> entry) {
    ConfigResource resource = entry._1;
    Config config = entry._2;

    Map<String, Model.BrokerConfig> brokerConfig =
        List.ofAll(config.entries())
            .toMap(
                ConfigEntry::name,
                configEntry ->
                    new Model.BrokerConfig(
                        configEntry.name(),
                        configEntry.value(),
                        configEntry.isDefault(),
                        configEntry.isReadOnly()));

    return new Broker(BrokerId.of(resource.name()), brokerConfig);
  }

  @SneakyThrows
  private Stream<ConfigResource> getConfigResources() {
    return admin.describeCluster().nodes().get().stream()
        .map(node -> new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node.id())));
  }

  @SneakyThrows
  private Map<String, Map<String, String>> getConfigs(Traversable<TopicListing> existingTopics) {
    return HashMap.ofAll(
            admin.describeConfigs(toConfigResource(existingTopics).toJavaSet()).all().get())
        .map(
            (configResource, config) ->
                Tuple.of(
                    configResource.name(),
                    List.ofAll(config.entries())
                        .filter(not(ConfigEntry::isDefault))
                        .toMap(ConfigEntry::name, ConfigEntry::value)));
  }

  public Traversable<ConfigResource> toConfigResource(Traversable<TopicListing> existingTopics) {
    return existingTopics.map(tl -> new ConfigResource(ConfigResource.Type.TOPIC, tl.name()));
  }

  @SneakyThrows
  public Traversable<Model.ExistingTopic> getExistingTopics() {

    Traversable<TopicListing> existingTopics = List.ofAll(admin.listTopics().listings().get());

    Map<String, Map<String, String>> topicNameToConfig = getConfigs(existingTopics);
    Map<String, Map<PartitionNumber, Traversable<Integer>>> partitionMap =
        getPartitions(existingTopics.map(TopicListing::name));

    return existingTopics.map(
        topic ->
            new ExistingTopic(
                TopicName.of(topic.name()),
                partitionMap
                    .get(topic.name())
                    .getOrElseThrow(
                        () ->
                            new RuntimeException(
                                "Not found partition info of topic " + topic.name())),
                topicNameToConfig
                    .get(topic.name())
                    .getOrElseThrow(
                        () ->
                            new RuntimeException(
                                "Not found configuration of topic " + topic.name()))));
  }

  @SneakyThrows
  private Map<String, Map<PartitionNumber, Traversable<Integer>>> getPartitions(
      Traversable<String> topicNames) {
    return HashMap.ofAll(admin.describeTopics(topicNames.toJavaSet()).all().get())
        .mapValues(this::toPartitionMap);
  }

  public Map<PartitionNumber, Traversable<Integer>> toPartitionMap(TopicDescription description) {
    return List.ofAll(description.partitions())
        .toMap(TopicPartitionInfo::partition, TopicPartitionInfo::replicas)
        .mapKeys(PartitionNumber::of)
        .mapValues(List::ofAll)
        .mapValues(nodes -> nodes.map(Node::id));
  }

  @SneakyThrows
  public boolean ongoingReassignment(int ongoingAssignmentRetries) {
    Map<TopicPartition, PartitionReassignment> ongoingAssignments;
    int retries = 0;
    do {
      ongoingAssignments = HashMap.ofAll(admin.listPartitionReassignments().reassignments().get());
      if (!ongoingAssignments.isEmpty()) {
        Thread.sleep(10L * (int) Math.pow(2, retries++));
      }
    } while (!ongoingAssignments.isEmpty() && retries < ongoingAssignmentRetries);

    return retries == ongoingAssignmentRetries;
  }

  public Model.Acl toAcl(Tuple2<ResourcePattern, List<AclBinding>> resourceToAcl) {
    ResourcePattern resourcePattern = resourceToAcl._1;
    List<AclBinding> aclBindings = resourceToAcl._2;

    return new Model.Acl(
        resourcePattern.resourceType().name(),
        resourcePattern.name(),
        resourcePattern.patternType().name(),
        aclBindings.map(
            aclBinding ->
                new Model.Permission(
                    aclBinding.entry().principal(),
                    aclBinding.entry().host(),
                    aclBinding.entry().operation().name(),
                    aclBinding.entry().permissionType().name())));
  }
}
