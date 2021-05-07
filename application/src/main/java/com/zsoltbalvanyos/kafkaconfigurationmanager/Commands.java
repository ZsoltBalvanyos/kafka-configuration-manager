package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.util.*;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.config.ConfigResource;

@Slf4j
@RequiredArgsConstructor
public class Commands {

  private final Admin admin;
  private final Mappers mappers;

  @SneakyThrows
  public void createTopics(Collection<RequiredTopic> topics) {
    List<NewTopic> newTopics = topics.stream().map(mappers::toNewTopic).collect(toList());
    log.debug("Creating topics: {}", newTopics);
    admin.createTopics(newTopics).all().get();
  }

  @SneakyThrows
  public void deleteTopics(Collection<String> topics) {
    log.debug("Deleting topics: {}", topics);
    admin.deleteTopics(topics).all().get();
  }

  @SneakyThrows
  public void updateConfigOfBrokers(Map<BrokerId, Map<String, Optional<String>>> updates) {
    var alterConfigRequest = mappers.getAlterConfigRequest(updates, ConfigResource.Type.BROKER);
    log.info("Modifying broker configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  @SneakyThrows
  public void updateConfigOfTopics(Map<TopicName, Map<String, Optional<String>>> updates) {
    var alterConfigRequest = mappers.getAlterConfigRequest(updates, ConfigResource.Type.TOPIC);
    log.debug("Modifying topic configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  @SneakyThrows
  public void updatePartitions(Map<TopicName, Integer> updates) {
    var newPartitions = mappers.toNewPartitions(updates);
    log.debug("Increasing partition count: {}", newPartitions);
    admin.createPartitions(newPartitions).all().get();
  }

  @SneakyThrows
  public void updateReplication(Map<TopicName, List<Partition>> updates) {
    var topicPartitions = mappers.toReassignment(updates);
    log.debug("Modifying replication: {}", topicPartitions);
    admin.alterPartitionReassignments(topicPartitions).all().get();
  }

  @SneakyThrows
  protected void createAcls(Collection<Acl> acls) {
    var aclBindings = acls.stream().flatMap(mappers::toAclBinding).collect(toSet());
    log.debug("Creating ACLs: {}", aclBindings);
    admin.createAcls(aclBindings).all().get();
  }

  @SneakyThrows
  public void deleteAcls(Collection<Acl> acls) {
    var filters = acls.stream().flatMap(mappers::toAclBindingFilter).collect(toSet());
    log.debug("Deleting ACLs: {}", filters);
    admin.deleteAcls(filters).all().get();
  }
}
