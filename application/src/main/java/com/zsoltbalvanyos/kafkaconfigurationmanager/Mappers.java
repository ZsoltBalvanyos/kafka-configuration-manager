package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import java.util.*;
import java.util.stream.Stream;
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

          alterConfigRequest.put(new ConfigResource(type, name.get()), alterConfigOps);
        });
    return alterConfigRequest;
  }

  public NewTopic toNewTopic(RequiredTopic topic) {
    return new NewTopic(
            topic.getName().get(),
            topic.getPartitionCount(),
            topic.getReplicationFactor().map(Integer::shortValue))
        .configs(topic.getConfig());
  }

  public Map<String, NewPartitions> toNewPartitions(Map<TopicName, Integer> updates) {
    Map<String, NewPartitions> newPartitions = new HashMap<>();
    updates.forEach(
        (topicName, partitions) ->
            newPartitions.put(topicName.get(), NewPartitions.increaseTo(partitions)));
    return newPartitions;
  }

  public Map<TopicPartition, Optional<NewPartitionReassignment>> toReassignment(
      Map<TopicName, List<Partition>> updates) {
    Map<TopicPartition, Optional<NewPartitionReassignment>> topicPartitions = new HashMap<>();
    updates.forEach(
        (topicName, partitions) ->
            partitions.forEach(
                partition ->
                    topicPartitions.put(
                        new TopicPartition(topicName.get(), partition.getPartitionNumber().get()),
                        Optional.of(new NewPartitionReassignment(partition.getReplicas())))));
    return topicPartitions;
  }

  public Stream<AclBindingFilter> toAclBindingFilter(Acl acl) {
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

  public Stream<AclBinding> toAclBinding(Acl acl) {
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
}
