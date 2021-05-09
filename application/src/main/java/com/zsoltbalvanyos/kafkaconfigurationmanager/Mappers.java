package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import io.vavr.Tuple2;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import java.util.Collection;
import java.util.Optional;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.*;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;

public class Mappers {

  public <T extends Id<String>>
      Map<ConfigResource, Collection<AlterConfigOp>> getAlterConfigRequest(
          Map<T, Map<String, Optional<String>>> updates, ConfigResource.Type type) {

    return updates.map(
        (name, requiredConfigs) ->
            Map.entry(
                new ConfigResource(type, name.get()),
                requiredConfigs
                    .map(this::getEntry)
                    .map(t -> new AlterConfigOp(t._1, t._2))
                    .asJava()));
  }

  private Tuple2<ConfigEntry, AlterConfigOp.OpType> getEntry(String key, Optional<String> value) {
    return Map.entry(
        new ConfigEntry(key, value.orElse("")),
        value.map(v -> AlterConfigOp.OpType.SET).orElse(AlterConfigOp.OpType.DELETE));
  }

  public NewTopic toNewTopic(RequiredTopic topic) {
    return new NewTopic(
            topic.getName().get(),
            topic.getPartitionCount(),
            topic.getReplicationFactor().map(Integer::shortValue))
        .configs(topic.getConfig().toJavaMap());
  }

  public Map<String, NewPartitions> toNewPartitions(Map<TopicName, Integer> updates) {
    return updates.map(
        (topicName, partitions) ->
            Map.entry(topicName.get(), NewPartitions.increaseTo(partitions)));
  }

  public Map<TopicPartition, Optional<NewPartitionReassignment>> toReassignment(
      Map<TopicName, Traversable<Partition>> updates) {
    return updates.flatMap(
        (topicName, partitions) ->
            partitions.toMap(
                partition ->
                    Map.entry(
                        new TopicPartition(topicName.get(), partition.getPartitionNumber().get()),
                        Optional.of(
                            new NewPartitionReassignment(partition.getReplicas().asJava())))));
  }

  public Traversable<AclBindingFilter> toAclBindingFilter(Acl acl) {
    return acl.getPermissions()
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

  public Traversable<AclBinding> toAclBinding(Acl acl) {
    return acl.getPermissions()
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
}
