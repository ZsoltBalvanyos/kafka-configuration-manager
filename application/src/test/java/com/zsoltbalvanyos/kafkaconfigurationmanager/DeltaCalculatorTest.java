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
  Collection<Broker> allBrokers = createBrokers(12);
  DeltaCalculator deltaCalculator = new DeltaCalculator(allBrokers);

  private List<Broker> createBrokers(int n) {
    return IntStream.range(0, n)
        .mapToObj(i -> new Broker(i + 1, Map.of()))
        .collect(Collectors.toList());
  }

  private List<Broker> toBroker(Collection<Integer> n) {
    return n.stream().map(i -> new Broker(i, Map.of())).collect(Collectors.toList());
  }

  @Test
  public void findNewAcls() {
    List<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toList());
    List<Acl> newAcls = random.objects(Acl.class, 5).collect(Collectors.toList());
    List<Acl> merged = new ArrayList<>(newAcls);
    merged.addAll(existingAcls);

    List<Acl> result = deltaCalculator.aclsToCreate(existingAcls, merged);

    assertThat(result).containsExactlyInAnyOrderElementsOf(newAcls);
  }

  @Test
  public void whenNoNewAcl_returnEmptySet() {
    List<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toList());

    List<Acl> result = deltaCalculator.aclsToCreate(existingAcls, existingAcls);

    assertThat(result).isEmpty();
  }

  @Test
  public void findAclsToDelete() {
    List<Acl> requiredAcls = random.objects(Acl.class, 10).collect(Collectors.toList());
    List<Acl> oldAcls = random.objects(Acl.class, 5).collect(Collectors.toList());

    List<Acl> merged = new ArrayList<>(oldAcls);
    merged.addAll(requiredAcls);

    List<Acl> result = deltaCalculator.aclsToDelete(merged, requiredAcls);

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldAcls);
  }

  @Test
  public void whenNoAclToDelete_returnEmptySet() {
    List<Acl> acls = random.objects(Acl.class, 10).collect(Collectors.toList());

    List<Acl> result = deltaCalculator.aclsToDelete(acls, acls);

    assertThat(result).isEmpty();
  }

  @Test
  public void findNewTopics() {
    List<Topic> existingTopics = random.objects(Topic.class, 10).collect(Collectors.toList());
    List<Topic> newTopics = random.objects(Topic.class, 5).collect(Collectors.toList());
    List<Topic> merged = new ArrayList<>(newTopics);
    merged.addAll(existingTopics);

    List<Topic> result = deltaCalculator.topicsToCreate(toExistingAll(existingTopics), merged);

    assertThat(result).containsExactlyInAnyOrderElementsOf(newTopics);
  }

  @Test
  public void whenNoNewTopic_returnEmptySet() {
    List<Topic> existingTopics = random.objects(Topic.class, 10).collect(Collectors.toList());

    List<Topic> result =
        deltaCalculator.topicsToCreate(toExistingAll(existingTopics), existingTopics);

    assertThat(result).isEmpty();
  }

  @Test
  public void whenNoTopic_returnEmptySet() {
    assertThat(deltaCalculator.topicsToCreate(List.of(), List.of())).isEmpty();
    assertThat(deltaCalculator.topicsToDelete(List.of(), List.of())).isEmpty();
    assertThat(deltaCalculator.aclsToCreate(List.of(), List.of())).isEmpty();
    assertThat(deltaCalculator.aclsToDelete(List.of(), List.of())).isEmpty();
  }

  @Test
  public void findTopicsToDelete() {
    List<Topic> requiredTopics = random.objects(Topic.class, 10).collect(Collectors.toList());
    List<ExistingTopic> oldTopics =
        random.objects(ExistingTopic.class, 5).collect(Collectors.toList());

    List<ExistingTopic> merged = new ArrayList<>(oldTopics);
    merged.addAll(toExistingAll(requiredTopics));

    Collection<ExistingTopic> result = deltaCalculator.topicsToDelete(merged, requiredTopics);

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldTopics);
  }

  @Test
  public void whenNoTopicToDelete_returnEmptyList() {
    List<Topic> topics = random.objects(Topic.class, 10).collect(Collectors.toList());
    List<ExistingTopic> existingTopics = toExistingAll(topics);

    Collection<ExistingTopic> result = deltaCalculator.topicsToDelete(existingTopics, topics);

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

    Map<String, List<Partition>> result =
        deltaCalculator.replicationUpdate(currentState, requiredState);

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "topic-1", List.of(new Partition(1, List.of(3, 5, 6))),
                "topic-3", List.of(new Partition(0, List.of(1, 2)))));
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

  private List<ExistingTopic> toExistingAll(List<Topic> topics) {
    return topics.stream().map(this::toExisting).collect(Collectors.toList());
  }

  private ExistingTopic toExisting(Topic topic) {
    List<Partition> partitions =
        IntStream.range(0, topic.getPartitionCount().orElse(1))
            .mapToObj(
                no ->
                    new Partition(
                        no,
                        IntStream.range(0, topic.getReplicationFactor().orElse(1))
                            .mapToObj(Integer::valueOf)
                            .collect(Collectors.toList())))
            .collect(Collectors.toList());

    return new ExistingTopic(topic.getName(), partitions, topic.getConfig());
  }
}
