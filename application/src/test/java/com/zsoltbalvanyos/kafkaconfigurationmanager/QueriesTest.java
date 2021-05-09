package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import io.vavr.collection.*;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
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

public class QueriesTest {

  EasyRandom random = TestUtil.randomizer();
  Admin admin = mock(Admin.class);
  Queries queries = new Queries(admin);

  @Test
  public void getAllBrokers() {
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
                java.util.Map.of(
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node1.id())),
                    config1,
                    new ConfigResource(ConfigResource.Type.BROKER, String.valueOf(node2.id())),
                    config2)));
    when(admin.describeConfigs(any())).thenReturn(describeConfigsResult);

    Traversable<Model.Broker> result = queries.getBrokers();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            HashSet.of(
                new Model.Broker(
                    Model.BrokerId.of("1"),
                    HashMap.ofAll(
                        config1.entries().stream()
                            .collect(
                                Collectors.toMap(
                                    ConfigEntry::name,
                                    c ->
                                        new Model.BrokerConfig(
                                            c.name(), c.value(), c.isDefault(), c.isReadOnly()))))),
                new Model.Broker(
                    Model.BrokerId.of("2"),
                    HashMap.ofAll(
                        config2.entries().stream()
                            .collect(
                                Collectors.toMap(
                                    ConfigEntry::name,
                                    c ->
                                        new Model.BrokerConfig(
                                            c.name(),
                                            c.value(),
                                            c.isDefault(),
                                            c.isReadOnly())))))));
  }

  @Test
  public void existingTopicConfigsFetched() {
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
                java.util.Map.of(
                    new ConfigResource(ConfigResource.Type.TOPIC, topicListing1.name()), config1,
                    new ConfigResource(ConfigResource.Type.TOPIC, topicListing2.name()), config2)));
    when(admin.describeConfigs(any())).thenReturn(describeConfigsResult);

    DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
    when(describeTopicsResult.all())
        .thenReturn(
            KafkaFuture.completedFuture(
                java.util.Map.of(
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

    Traversable<Model.ExistingTopic> result = queries.getExistingTopics();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            List.of(
                new Model.ExistingTopic(
                    Model.TopicName.of(topicListing1.name()),
                    HashMap.of(Model.PartitionNumber.of(0), io.vavr.collection.List.of(0)),
                    HashMap.ofAll(
                        config1.entries().stream()
                            .filter(ce -> !ce.isDefault())
                            .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))),
                new Model.ExistingTopic(
                    Model.TopicName.of(topicListing2.name()),
                    HashMap.of(Model.PartitionNumber.of(0), io.vavr.collection.List.of(0)),
                    HashMap.ofAll(
                        config2.entries().stream()
                            .filter(ce -> !ce.isDefault())
                            .collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))))));
  }

  @Test
  public void givenAcls_whenListAcls_thenAllReturned() {
    var aclBindings = random.objects(AclBinding.class, 10).collect(Collectors.toSet());

    DescribeAclsResult describeAclsResult = mock(DescribeAclsResult.class);
    when(describeAclsResult.values()).thenReturn(KafkaFuture.completedFuture(aclBindings));
    when(admin.describeAcls(any())).thenReturn(describeAclsResult);

    var result = queries.getAcls();

    assertThat(aclBindings.stream().map(AclBinding::pattern).map(ResourcePattern::name))
        .containsExactlyInAnyOrderElementsOf(result.map(Model.Acl::getName));
    assertThat(
            aclBindings.stream()
                .map(AclBinding::pattern)
                .map(ResourcePattern::resourceType)
                .map(ResourceType::name))
        .containsExactlyInAnyOrderElementsOf(result.map(Model.Acl::getResourceType));
    assertThat(
            aclBindings.stream()
                .map(AclBinding::pattern)
                .map(ResourcePattern::patternType)
                .map(PatternType::name))
        .containsExactlyInAnyOrderElementsOf(result.map(Model.Acl::getPatternType));
  }

  @Test
  public void givenBroker_whenAuthorizerNotSet_emptyCollectionOfAclsReturned() {
    when(admin.describeAcls(any()))
        .thenAnswer(
            invocation -> {
              throw new ExecutionException(new SecurityDisabledException(""));
            });

    var result = queries.getAcls();

    assertThat(result).isEmpty();
  }

  @Test
  public void givenBroker_whenOngoingAssignment_waitTillAssignmentDone() {
    ListPartitionReassignmentsResult listPartitionReassignmentsResult =
        mock(ListPartitionReassignmentsResult.class);

    KafkaFuture<java.util.Map<TopicPartition, PartitionReassignment>> future1 =
        KafkaFuture.completedFuture(
            java.util.Map.of(
                new TopicPartition("topic-1", 0),
                new PartitionReassignment(List.of(1), List.of(2), List.of(3))));

    KafkaFuture<java.util.Map<TopicPartition, PartitionReassignment>> future2 =
        KafkaFuture.completedFuture(
            java.util.Map.of(
                new TopicPartition("topic-1", 0),
                new PartitionReassignment(List.of(1), List.of(2), List.of(3))));

    KafkaFuture<java.util.Map<TopicPartition, PartitionReassignment>> future3 =
        KafkaFuture.completedFuture(java.util.Map.of());

    when(listPartitionReassignmentsResult.reassignments())
        .thenReturn(future1)
        .thenReturn(future2)
        .thenReturn(future3);

    when(admin.listPartitionReassignments())
        .thenReturn(
            listPartitionReassignmentsResult,
            listPartitionReassignmentsResult,
            listPartitionReassignmentsResult);

    assertThat(queries.ongoingReassignment(10)).isFalse();
    verify(admin, times(3)).listPartitionReassignments();
  }

  @Test
  public void givenBroker_whenOngoingAssignment_timeout() {
    ListPartitionReassignmentsResult listPartitionReassignmentsResult =
        mock(ListPartitionReassignmentsResult.class);

    when(listPartitionReassignmentsResult.reassignments())
        .thenReturn(
            KafkaFuture.completedFuture(
                java.util.Map.of(
                    new TopicPartition("topic-1", 0),
                    new PartitionReassignment(List.of(1), List.of(2), List.of(3)))));
    when(admin.listPartitionReassignments()).thenReturn(listPartitionReassignmentsResult);

    assertThat(queries.ongoingReassignment(5)).isTrue();
    verify(admin, times(5)).listPartitionReassignments();
  }
}
