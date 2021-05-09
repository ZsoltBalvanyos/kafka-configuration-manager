package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import java.util.Optional;
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
  public void createTopics(Traversable<RequiredTopic> topics) {
    var newTopics = topics.map(mappers::toNewTopic).toJavaSet();
    log.debug("Creating topics: {}", newTopics);
    admin.createTopics(newTopics).all().get();
  }

  @SneakyThrows
  public void deleteTopics(Traversable<String> topics) {
    log.debug("Deleting topics: {}", topics);
    admin.deleteTopics(topics.toJavaSet()).all().get();
  }

  @SneakyThrows
  public void updateConfigOfBrokers(Map<BrokerId, Map<String, Optional<String>>> updates) {
    var alterConfigRequest =
        mappers.getAlterConfigRequest(updates, ConfigResource.Type.BROKER).toJavaMap();
    log.info("Modifying broker configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  @SneakyThrows
  public void updateConfigOfTopics(Map<TopicName, Map<String, Optional<String>>> updates) {
    var alterConfigRequest =
        mappers.getAlterConfigRequest(updates, ConfigResource.Type.TOPIC).toJavaMap();
    log.debug("Modifying topic configurations: {}", alterConfigRequest);
    admin.incrementalAlterConfigs(alterConfigRequest).all().get();
  }

  @SneakyThrows
  public void updatePartitions(Map<TopicName, Integer> updates) {
    var newPartitions = mappers.toNewPartitions(updates).toJavaMap();
    log.debug("Increasing partition count: {}", newPartitions);
    admin.createPartitions(newPartitions).all().get();
  }

  @SneakyThrows
  public void updateReplication(Map<TopicName, Traversable<Partition>> updates) {
    var topicPartitions = mappers.toReassignment(updates).toJavaMap();
    log.debug("Modifying replication: {}", topicPartitions);
    admin.alterPartitionReassignments(topicPartitions).all().get();
  }

  @SneakyThrows
  protected void createAcls(Traversable<Acl> acls) {
    var aclBindings = acls.flatMap(mappers::toAclBinding).toJavaSet();
    log.debug("Creating ACLs: {}", aclBindings);
    admin.createAcls(aclBindings).all().get();
  }

  @SneakyThrows
  public void deleteAcls(Traversable<Acl> acls) {
    var filters = acls.flatMap(mappers::toAclBindingFilter).toJavaSet();
    log.debug("Deleting ACLs: {}", filters);
    admin.deleteAcls(filters).all().get();
  }
}
