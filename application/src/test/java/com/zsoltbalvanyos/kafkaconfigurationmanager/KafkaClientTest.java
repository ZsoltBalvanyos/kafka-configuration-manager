package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.SecurityDisabledException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourceType;
import org.jeasy.random.EasyRandom;
import org.junit.Test;

public class KafkaClientTest {

  EasyRandom random = TestUtil.randomizer;

  Admin admin = mock(Admin.class);
  KafkaFuture<Void> kafkaFuture = mock(KafkaFuture.class);

  KafkaClient kafkaClient = new KafkaClient(admin, 5);

  @Test
  public void getAllBrokers() throws ExecutionException, InterruptedException {
    Node node1 = new Node(1, "localhost", 1234);
    Node node2 = new Node(2, "localhost", 4567);
    Config config1 = random.nextObject(Config.class);
    Config config2 = random.nextObject(Config.class);

    DescribeClusterResult describeClusterResult = mock(DescribeClusterResult.class);
    when(describeClusterResult.nodes())
        .thenReturn(KafkaFuture.completedFuture(List.of(node1, node2)));
    when(admin.describeCluster()).thenReturn(describeClusterResult);

    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    when(describeConfigsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node1.id())),
                        config1,
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node2.id())),
                        config2)));
    when(admin.describeConfigs(any())).thenReturn(describeConfigsResult);

    Set<Model.Broker> result = kafkaClient.getAllBrokers();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            Set.of(
                new Model.Broker(
                    1,
                    config1.entries().stream()
                        .collect(
                            Collectors.toMap(
                                ConfigEntry::name,
                                c ->
                                    new BrokerConfig(
                                        c.name(), c.value(), c.isDefault(), c.isReadOnly())))),
                new Model.Broker(
                    2,
                    config2.entries().stream()
                        .collect(
                            Collectors.toMap(
                                ConfigEntry::name,
                                c ->
                                    new BrokerConfig(
                                        c.name(), c.value(), c.isDefault(), c.isReadOnly()))))));
  }

  @Test
  public void existingTopicConfigsFetched() throws ExecutionException, InterruptedException {
    TopicListing topicListing1 = new TopicListing("topicListing1", false);
    TopicListing topicListing2 = new TopicListing("topicListing2", false);
    Config config1 = random.nextObject(Config.class);
    Config config2 = random.nextObject(Config.class);

    ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
    when(listTopicsResult.listings())
        .thenReturn(KafkaFuture.completedFuture(List.of(topicListing1, topicListing2)));

    when(admin.listTopics()).thenReturn(listTopicsResult);

    DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
    when(describeConfigsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, topicListing1.name()), config1,
                    new ConfigResource(ConfigResource.Type.TOPIC, topicListing2.name()), config2)));
    when(admin.describeConfigs(any())).thenReturn(describeConfigsResult);

    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    when(describeTopicsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    topicListing1.name(),
                        new TopicDescription(
                            topicListing1.name(),
                            false,
                            List.of(
                                new TopicPartitionInfo(
                                    0, null, List.of(new Node(0, "", 0)), List.of()))),
                    topicListing2.name(),
                        new TopicDescription(
                            topicListing2.name(),
                            false,
                            List.of(
                                new TopicPartitionInfo(
                                    0, null, List.of(new Node(0, "", 0)), List.of()))))));
    when(admin.describeTopics(any())).thenReturn(describeTopicsResult);

    List<ExistingTopic> result = kafkaClient.getExistingTopics();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                new ExistingTopic(
                    topicListing1.name(),
                    List.of(new Partition(0, List.of(0))),
                    config1.entries().stream()
                        .filter(ce -> !ce.isDefault())
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))),
                new ExistingTopic(
                    topicListing2.name(),
                    List.of(new Partition(0, List.of(0))),
                    config2.entries().stream()
                        .filter(ce -> !ce.isDefault())
                        .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))));
  }

  @Test
  public void updateRequestBuild() {
    Map<String, Map<String, Optional<String>>> update =
        Map.of(
            "topic1",
                Map.of(
                    "config1", Optional.of("value1"),
                    "config2", Optional.of("value2")),
            "topic2",
                Map.of(
                    "config3", Optional.empty(),
                    "config4", Optional.of("value4")));

    Map<ConfigResource, Collection<AlterConfigOp>> result =
        kafkaClient.getAlterConfigRequest(update, ConfigResource.Type.TOPIC);

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                    Set.of(
                        new AlterConfigOp(
                            new ConfigEntry("config1", "value1"), AlterConfigOp.OpType.SET),
                        new AlterConfigOp(
                            new ConfigEntry("config2", "value2"), AlterConfigOp.OpType.SET)),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic2"),
                    Set.of(
                        new AlterConfigOp(
                            new ConfigEntry("config3", ""), AlterConfigOp.OpType.DELETE),
                        new AlterConfigOp(
                            new ConfigEntry("config4", "value4"), AlterConfigOp.OpType.SET))));
  }

  @Test
  public void givenListOfTopics_whenCreateTopicsCalled_thenAdminCreateTopicsCalled() {
    var topics = random.objects(Topic.class, 10).collect(Collectors.toList());
    var newTopics = topics.stream().map(kafkaClient::toNewTopic).collect(Collectors.toList());

    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    when(createTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.createTopics(eq(newTopics))).thenReturn(createTopicsResult);

    kafkaClient.createTopics(topics);
    verify(admin, times(1)).createTopics(eq(newTopics));
  }

  @Test
  public void givenListOfTopicNames_whenDeleteTopicsCalled_thenAdminDeleteTopicsCalled() {
    var topicNames = random.objects(String.class, 10).collect(Collectors.toList());

    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.deleteTopics(eq(topicNames))).thenReturn(deleteTopicsResult);

    kafkaClient.deleteTopics(topicNames);
    verify(admin, times(1)).deleteTopics(topicNames);
  }

  @Test
  public void givenAcls_whenCreateAcls_thenAdminCreateAclsCalled() {
    var acls =
        random
            .objects(Acl.class, 10)
            .map(a -> a.withResourceType(ResourceType.TOPIC.name()))
            .collect(Collectors.toSet());
    var newAcls = acls.stream().flatMap(kafkaClient::toAclBinding).collect(Collectors.toSet());

    CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
    when(createAclsResult.all()).thenReturn(kafkaFuture);
    when(admin.createAcls(eq(newAcls))).thenReturn(createAclsResult);

    kafkaClient.createAcls(acls);
    verify(admin, times(1)).createAcls(eq(newAcls));
  }

  @Test
  public void givenAcls_whenDeleteAcls_thenAdminDeleteAclsCalled() {
    var acls =
        random
            .objects(Acl.class, 10)
            .map(a -> a.withResourceType(ResourceType.TOPIC.name()))
            .collect(Collectors.toSet());
    var aclsToDelete =
        acls.stream().flatMap(kafkaClient::toAclBindingFilter).collect(Collectors.toSet());

    AclBinding aclBinding = mock(AclBinding.class);
    DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);
    when(deleteAclsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(aclBinding)));
    when(admin.deleteAcls(eq(aclsToDelete))).thenReturn(deleteAclsResult);

    kafkaClient.deleteAcls(acls);
    verify(admin, times(1)).deleteAcls(eq(aclsToDelete));
  }

  @Test
  public void givenAcls_whenListAcls_thenAllReturned() {
    var aclBindings = random.objects(AclBinding.class, 10).collect(Collectors.toSet());

    DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
    when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(aclBindings));
    when(admin.describeAcls(any())).thenReturn(describeAclsResult);

    var result = kafkaClient.getAcls();

    assertThat(aclBindings.stream().map(AclBinding::pattern).map(ResourcePattern::name))
        .containsExactlyInAnyOrderElementsOf(
            result.stream().map(Acl::getName).collect(Collectors.toList()));
    assertThat(
            aclBindings.stream()
                .map(AclBinding::pattern)
                .map(ResourcePattern::resourceType)
                .map(ResourceType::name))
        .containsExactlyInAnyOrderElementsOf(
            result.stream().map(Acl::getResourceType).collect(Collectors.toList()));
    assertThat(
            aclBindings.stream()
                .map(AclBinding::pattern)
                .map(ResourcePattern::patternType)
                .map(PatternType::name))
        .containsExactlyInAnyOrderElementsOf(
            result.stream().map(Acl::getPatternType).collect(Collectors.toList()));
  }

  @Test
  public void givenBroker_whenAuthorizerNotSet_emptyCollectionOfAclsReturned() {
    when(admin.describeAcls(any()))
        .thenAnswer(
            invocation -> {
              throw new ExecutionException(new SecurityDisabledException(""));
            });

    var result = kafkaClient.getAcls();

    assertThat(result).isEmpty();
  }

  @Test
  public void givenBroker_whenOngoingAssignment_waitTillAssignmentDone() {
    ListPartitionReassignmentsResult listPartitionReassignmentsResult =
        mock(ListPartitionReassignmentsResult.class);
    when(listPartitionReassignmentsResult.reassignments())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    new TopicPartition("topic-1", 0),
                    new PartitionReassignment(List.of(1), List.of(2), List.of(3)))),
            KafkaFuture.completedFuture(
                Map.of(
                    new TopicPartition("topic-1", 0),
                    new PartitionReassignment(List.of(1), List.of(2), List.of(3)))),
            KafkaFuture.completedFuture(Map.of()));
    when(admin.listPartitionReassignments())
        .thenReturn(
            listPartitionReassignmentsResult,
            listPartitionReassignmentsResult,
            listPartitionReassignmentsResult);
    assertThat(kafkaClient.ongoingReassignment()).isFalse();
    verify(admin, times(3)).listPartitionReassignments();
  }

  @Test
  public void givenBroker_whenOngoingAssignment_timeout() {
    ListPartitionReassignmentsResult listPartitionReassignmentsResult =
        mock(ListPartitionReassignmentsResult.class);
    when(listPartitionReassignmentsResult.reassignments())
        .thenReturn(
            KafkaFuture.completedFuture(
                Map.of(
                    new TopicPartition("topic-1", 0),
                    new PartitionReassignment(List.of(1), List.of(2), List.of(3)))));
    when(admin.listPartitionReassignments()).thenReturn(listPartitionReassignmentsResult);
    assertThat(kafkaClient.ongoingReassignment()).isTrue();
    verify(admin, times(5)).listPartitionReassignments();
  }
}
