package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jeasy.random.EasyRandom;
import org.junit.Test;

public class DeltaCalculatorTest {

  EasyRandom random = TestUtil.randomizer;
  Set<Broker> allBrokers = createBrokers(12);
  DeltaCalculator deltaCalculator = new DeltaCalculator(allBrokers);

  private Set<Broker> createBrokers(int n) {
    return IntStream.range(0, n)
        .mapToObj(i -> new Broker(i + 1, Map.of()))
        .collect(Collectors.toSet());
  }

  private Set<Broker> toBroker(Collection<Integer> n) {
    return n.stream().map(i -> new Broker(i, Map.of())).collect(Collectors.toSet());
  }

  @Test
  public void findNewAcls() {
    Set<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toSet());
    Set<Acl> newAcls = random.objects(Acl.class, 5).collect(Collectors.toSet());
    Set<Acl> merged = new HashSet<>(newAcls);
    merged.addAll(existingAcls);

    Set<Acl> result = deltaCalculator.aclsToCreate(existingAcls, merged);

    assertThat(result).containsExactlyInAnyOrderElementsOf(newAcls);
  }

  @Test
  public void whenNoNewAcl_returnEmptySet() {
    Set<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toSet());

    Set<Acl> result = deltaCalculator.aclsToCreate(existingAcls, existingAcls);

    assertThat(result).isEmpty();
  }

  @Test
  public void findAclsToDelete() {
    Set<Acl> requiredAcls = random.objects(Acl.class, 10).collect(Collectors.toSet());
    Set<Acl> oldAcls = random.objects(Acl.class, 5).collect(Collectors.toSet());

    Set<Acl> merged = new HashSet<>(oldAcls);
    merged.addAll(requiredAcls);

    Set<Acl> result = deltaCalculator.aclsToDelete(merged, requiredAcls);

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldAcls);
  }

  @Test
  public void whenNoAclToDelete_returnEmptySet() {
    Set<Acl> acls = random.objects(Acl.class, 10).collect(Collectors.toSet());

    Set<Acl> result = deltaCalculator.aclsToDelete(acls, acls);

    assertThat(result).isEmpty();
  }

  @Test
  public void findNewTopics() {
    Set<Topic> existingTopics = random.objects(Topic.class, 10).collect(Collectors.toSet());
    Set<Topic> newTopics = random.objects(Topic.class, 5).collect(Collectors.toSet());
    Set<Topic> merged = new HashSet<>(newTopics);
    merged.addAll(existingTopics);

    Set<Topic> result = deltaCalculator.topicsToCreate(toExistingAll(existingTopics), merged);

    assertThat(result).containsExactlyInAnyOrderElementsOf(newTopics);
  }

  @Test
  public void whenNoNewTopic_returnEmptySet() {
    Set<Topic> existingTopics = random.objects(Topic.class, 10).collect(Collectors.toSet());

    Set<Topic> result =
        deltaCalculator.topicsToCreate(toExistingAll(existingTopics), existingTopics);

    assertThat(result).isEmpty();
  }

  @Test
  public void whenNoTopic_returnEmptySet() {
    assertThat(deltaCalculator.topicsToCreate(Set.of(), Set.of())).isEmpty();
    assertThat(deltaCalculator.topicsToDelete(Set.of(), Set.of())).isEmpty();
    assertThat(deltaCalculator.aclsToCreate(Set.of(), Set.of())).isEmpty();
    assertThat(deltaCalculator.aclsToDelete(Set.of(), Set.of())).isEmpty();
  }

  @Test
  public void findTopicsToDelete() {
    Set<Topic> requiredTopics = random.objects(Topic.class, 10).collect(Collectors.toSet());
    Set<ExistingTopic> oldTopics =
        random.objects(ExistingTopic.class, 5).collect(Collectors.toSet());

    Set<ExistingTopic> merged = new HashSet<>(oldTopics);
    merged.addAll(toExistingAll(requiredTopics));

    Set<ExistingTopic> result = deltaCalculator.topicsToDelete(merged, requiredTopics);

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldTopics);
  }

  @Test
  public void whenNoTopicToDelete_returnEmptySet() {
    Set<Topic> topics = random.objects(Topic.class, 10).collect(Collectors.toSet());
    Set<ExistingTopic> existingTopics = toExistingAll(topics);

    Set<ExistingTopic> result = deltaCalculator.topicsToDelete(existingTopics, topics);

    assertThat(result).isEmpty();
  }

  @Test
  public void testUpdate() {

    Topic topicToCreate = random.nextObject(Topic.class);
    Topic topicToDelete = random.nextObject(Topic.class);

    Topic remainingTopic1 =
        random
            .nextObject(Topic.class)
            .withConfig(Map.of("key1a", "value1a", "key1b", "value1b", "key1c", "value1c"));
    Topic remainingTopic2 =
        random
            .nextObject(Topic.class)
            .withConfig(Map.of("key2a", "value2a", "key2b", "value2b", "key2c", "value2c"));

    Topic remainingTopicWithUpdatedConfig =
        remainingTopic2.withConfig(Map.of("key2b", "value2b", "key2c", "value2x"));

    Set<ExistingTopic> currentState = new HashSet<>();
    currentState.add(toExisting(topicToDelete));
    currentState.add(toExisting(remainingTopic1));
    currentState.add(toExisting(remainingTopic2));

    Set<Topic> requiredState = new HashSet<>();
    requiredState.add(topicToCreate);
    requiredState.add(remainingTopic1);
    requiredState.add(remainingTopicWithUpdatedConfig);

    Map<String, Map<String, Optional<String>>> result =
        deltaCalculator.topicConfigUpdate(currentState, requiredState);

    assertThat(result.get(remainingTopicWithUpdatedConfig.getName()))
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "key2a", Optional.empty(),
                "key2c", Optional.of("value2x")));
  }

  @Test
  public void whenReplicationFactorIsIncreased_extraBrokersAreAdded() {
    List<Integer> availableBrokers = List.of(0, 1, 2, 3, 4, 5);
    List<Integer> currentBrokers = List.of(6, 7, 8, 9);

    List<Integer> allBrokers = new ArrayList<>();
    allBrokers.addAll(availableBrokers);
    allBrokers.addAll(currentBrokers);

    List<Integer> result =
        new DeltaCalculator(toBroker(allBrokers)).selectBrokersForReplication(currentBrokers, 7);

    assertThat(result).hasSize(7);
    List<Integer> addedBrokers = new ArrayList<>();
    addedBrokers.addAll(result);
    addedBrokers.removeAll(currentBrokers);
    assertThat(addedBrokers).hasSize(3);
    assertThat(availableBrokers).containsAll(addedBrokers);
  }

  @Test
  public void whenReplicationFactorIsLowered_unneededBrokersAreRemoved() {
    List<Integer> currentBrokers = List.of(1, 2, 3, 4, 5, 6, 7);

    List<Integer> result =
        new DeltaCalculator(allBrokers).selectBrokersForReplication(currentBrokers, 4);

    assertThat(result).hasSize(4);
    assertThat(currentBrokers).containsAll(result);
  }

  @Test
  public void whenReplicationFactorIsUnchanged_currentStateIsReturned() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);
    List<Integer> result =
        new DeltaCalculator(toBroker(currentBrokers))
            .selectBrokersForReplication(currentBrokers, currentBrokers.size());

    assertThat(result).containsExactlyInAnyOrderElementsOf(currentBrokers);
  }

  @Test
  public void whenCurrentBrokersIsNotSubsetOfAllBrokers_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 14);

    assertThatThrownBy(
            () ->
                deltaCalculator.selectBrokersForReplication(currentBrokers, currentBrokers.size()))
        .hasMessageContaining("Invalid replication state");
  }

  @Test
  public void whenReplicationFactorIsLessThanOne_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);

    assertThatThrownBy(() -> deltaCalculator.selectBrokersForReplication(currentBrokers, 0))
        .hasMessage("Replication factor must be greater than 0");
  }

  @Test
  public void whenReplicationFactorGreaterThanAllBrokers_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);

    assertThatThrownBy(
            () ->
                deltaCalculator.selectBrokersForReplication(currentBrokers, allBrokers.size() + 1))
        .hasMessage(
            String.format(
                "Replication factor [%d] must not be greater than the number of brokers [%d]",
                allBrokers.size() + 1, allBrokers.size()));
  }

  @Test
  public void whenPartitionCountIncreased_topicWithNewPartitionCountIncluded() {
    Set<ExistingTopic> currentState =
        Set.of(
            new ExistingTopic("topic-1", Set.of(new Partition(0, List.of(0))), Map.of()),
            new ExistingTopic("topic-2", Set.of(new Partition(0, List.of(0))), Map.of()),
            new ExistingTopic("topic-3", Set.of(new Partition(0, List.of(0))), Map.of()));

    Set<Topic> requiredState =
        Set.of(
            new Topic("topic-1", Optional.of(1), Optional.of(1), Map.of()),
            new Topic("topic-2", Optional.of(4), Optional.of(1), Map.of()),
            new Topic("topic-3", Optional.of(2), Optional.of(1), Map.of()));

    Map<String, Integer> result = deltaCalculator.partitionUpdate(currentState, requiredState);

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "topic-2", 4,
                "topic-3", 2));
  }

  @Test
  public void whenPartitionCountDecreased_errorThrown() {
    Set<ExistingTopic> currentState =
        Set.of(
            new ExistingTopic("topic-1", Set.of(new Partition(0, List.of(0))), Map.of()),
            new ExistingTopic("topic-2", Set.of(new Partition(0, List.of(0))), Map.of()),
            new ExistingTopic(
                "topic-3",
                Set.of(
                    new Partition(0, List.of(0)),
                    new Partition(1, List.of(0)),
                    new Partition(2, List.of(0))),
                Map.of()));

    Set<Topic> requiredState =
        Set.of(
            new Topic("topic-1", Optional.of(1), Optional.of(1), Map.of()),
            new Topic("topic-2", Optional.of(4), Optional.of(1), Map.of()),
            new Topic("topic-3", Optional.of(2), Optional.of(1), Map.of()));

    assertThatThrownBy(() -> deltaCalculator.partitionUpdate(currentState, requiredState))
        .hasMessage(
            "Number of partitions cannot be lowered. Current number of partitions: 3, requested number of partitions: 2");
  }

  @Test
  public void testReplication() {
    Set<ExistingTopic> currentState =
        Set.of(
            new ExistingTopic(
                "topic-1",
                Set.of(new Partition(0, List.of(1, 3, 4)), new Partition(1, List.of(3))),
                Map.of()),
            new ExistingTopic("topic-3", Set.of(new Partition(0, List.of(1, 2, 3))), Map.of()));

    Set<Topic> requiredState =
        Set.of(
            new Topic("topic-1", Optional.of(2), Optional.of(3), Map.of()),
            new Topic("topic-3", Optional.of(1), Optional.of(2), Map.of()));

    DeltaCalculator deltaCalculator = mock(DeltaCalculator.class);
    when(deltaCalculator.selectBrokersForReplication(eq(List.of(1, 3, 4)), eq(3)))
        .thenReturn(List.of(1, 3, 4));
    when(deltaCalculator.selectBrokersForReplication(eq(List.of(3)), eq(3)))
        .thenReturn(List.of(3, 5, 6));
    when(deltaCalculator.selectBrokersForReplication(eq(List.of(1, 2, 3)), eq(2)))
        .thenReturn(List.of(1, 2));
    when(deltaCalculator.replicationUpdate(currentState, requiredState)).thenCallRealMethod();

    Map<String, Collection<Partition>> result =
        deltaCalculator.replicationUpdate(currentState, requiredState);

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "topic-1", Set.of(new Partition(1, List.of(3, 5, 6))),
                "topic-3", Set.of(new Partition(0, List.of(1, 2)))));
  }

  @Test
  public void givenBrokerConfiguration_whenConfigRemoved_brokerIsSetToDefault() {
    Broker broker =
        new Broker(
            1,
            Map.of(
                "key1", new BrokerConfig("key1", "value1", false, false),
                "key2", new BrokerConfig("key2", "value2", false, false),
                "key3", new BrokerConfig("key3", "value3", false, false)));

    var result =
        new DeltaCalculator(Set.of(broker))
            .brokerConfigUpdate(
                Map.of(
                    "key1", "value1",
                    "key3", "value3-new"));

    assertThat(result)
        .isEqualTo(
            Map.of(
                "1",
                Map.of(
                    "key2", Optional.empty(),
                    "key3", Optional.of("value3-new"))));
  }

  private Set<ExistingTopic> toExistingAll(Set<Topic> topics) {
    return topics.stream().map(this::toExisting).collect(Collectors.toSet());
  }

  private ExistingTopic toExisting(Topic topic) {
    Set<Partition> partitions =
        IntStream.range(0, topic.getPartitionCount().orElse(1))
            .mapToObj(
                no ->
                    new Partition(
                        no,
                        IntStream.range(0, topic.getReplicationFactor().orElse(1))
                            .mapToObj(Integer::valueOf)
                            .collect(Collectors.toList())))
            .collect(Collectors.toSet());

    return new ExistingTopic(topic.getName(), partitions, topic.getConfig());
  }
}
