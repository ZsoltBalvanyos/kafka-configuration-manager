package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import io.vavr.Tuple;
import io.vavr.collection.Stream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jeasy.random.EasyRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeltaCalculatorTest {

  CurrentState currentState = new CurrentState(Set.of(), createBrokers(12), Set.of());
  RequiredState requiredState = new RequiredState(Set.of(), Map.of(), Set.of());

  EasyRandom random = TestUtil.randomizer;
  DeltaCalculator deltaCalculator = new DeltaCalculator(currentState, requiredState);

  private List<Broker> createBrokers(int n) {
    return IntStream.range(0, n)
        .mapToObj(i -> new Broker(BrokerId.of(String.valueOf(i + 1)), Map.of()))
        .collect(Collectors.toList());
  }

  private List<Broker> toBroker(Collection<Integer> n) {
    return n.stream()
        .map(String::valueOf)
        .map(i -> new Broker(BrokerId.of(i), Map.of()))
        .collect(Collectors.toList());
  }

  @Test
  public void findNewAcls() {
    List<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toList());
    List<Acl> newAcls = random.objects(Acl.class, 5).collect(Collectors.toList());
    List<Acl> merged = new ArrayList<>(newAcls);
    merged.addAll(existingAcls);

    var deltaCalculator =
        new DeltaCalculator(currentState.withAcls(existingAcls), requiredState.withAcls(merged));
    List<Acl> result = deltaCalculator.aclsToCreate();

    assertThat(result).containsExactlyInAnyOrderElementsOf(newAcls);
  }

  @Test
  public void whenNoNewAcl_returnEmptySet() {
    List<Acl> existingAcls = random.objects(Acl.class, 10).collect(Collectors.toList());

    var deltaCalculator = new DeltaCalculator(currentState.withAcls(existingAcls), requiredState);
    List<Acl> result = deltaCalculator.aclsToCreate();

    assertThat(result).isEmpty();
  }

  @Test
  public void findAclsToDelete() {
    List<Acl> requiredAcls = random.objects(Acl.class, 10).collect(Collectors.toList());
    List<Acl> oldAcls = random.objects(Acl.class, 5).collect(Collectors.toList());

    List<Acl> merged = new ArrayList<>(oldAcls);
    merged.addAll(requiredAcls);

    var deltaCalculator =
        new DeltaCalculator(currentState.withAcls(merged), requiredState.withAcls(requiredAcls));
    List<Acl> result = deltaCalculator.aclsToDelete();

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldAcls);
  }

  @Test
  public void whenNoAclToDelete_returnEmptySet() {
    List<Acl> acls = random.objects(Acl.class, 10).collect(Collectors.toList());

    var deltaCalculator =
        new DeltaCalculator(currentState.withAcls(acls), requiredState.withAcls(acls));
    List<Acl> result = deltaCalculator.aclsToDelete();

    assertThat(result).isEmpty();
  }

  @Test
  public void findNewTopics() {
    List<RequiredTopic> existingTopics =
        random.objects(RequiredTopic.class, 10).collect(Collectors.toList());
    List<RequiredTopic> newTopics =
        random.objects(RequiredTopic.class, 5).collect(Collectors.toList());
    List<RequiredTopic> merged = new ArrayList<>(newTopics);
    merged.addAll(existingTopics);

    var deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(toExistingAll(existingTopics)),
            requiredState.withTopics(merged));
    List<RequiredTopic> result = deltaCalculator.topicsToCreate();

    assertThat(result).containsExactlyInAnyOrderElementsOf(newTopics);
  }

  @Test
  public void whenNoNewTopic_returnEmptySet() {
    List<RequiredTopic> requiredTopics =
        random.objects(RequiredTopic.class, 10).collect(Collectors.toList());

    var deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(toExistingAll(requiredTopics)),
            requiredState.withTopics(requiredTopics));
    List<RequiredTopic> result = deltaCalculator.topicsToCreate();

    assertThat(result).isEmpty();
  }

  @Test
  public void whenNoTopic_returnEmptySet() {
    assertThat(deltaCalculator.topicsToCreate()).isEmpty();
    assertThat(deltaCalculator.topicsToDelete()).isEmpty();
    assertThat(deltaCalculator.aclsToCreate()).isEmpty();
    assertThat(deltaCalculator.aclsToDelete()).isEmpty();
  }

  @Test
  public void findTopicsToDelete() {
    List<RequiredTopic> requiredTopics =
        random.objects(RequiredTopic.class, 10).collect(Collectors.toList());
    List<ExistingTopic> oldTopics =
        random.objects(ExistingTopic.class, 5).collect(Collectors.toList());

    List<ExistingTopic> merged = new ArrayList<>(oldTopics);
    merged.addAll(toExistingAll(requiredTopics));

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(merged), requiredState.withTopics(requiredTopics));
    Collection<ExistingTopic> result = deltaCalculator.topicsToDelete();

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldTopics);
  }

  @Test
  public void whenNoTopicToDelete_returnEmptyList() {
    List<RequiredTopic> topics =
        random.objects(RequiredTopic.class, 10).collect(Collectors.toList());
    List<ExistingTopic> existingTopics = toExistingAll(topics);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(existingTopics), requiredState.withTopics(topics));
    Collection<ExistingTopic> result = deltaCalculator.topicsToDelete();

    assertThat(result).isEmpty();
  }

  @Test
  public void testTopicConfigurationUpdate() {

    RequiredTopic topicToCreate = random.nextObject(RequiredTopic.class);
    RequiredTopic topicToDelete = random.nextObject(RequiredTopic.class);

    RequiredTopic remainingTopic1 =
        random
            .nextObject(RequiredTopic.class)
            .withConfig(Map.of("key1a", "value1a", "key1b", "value1b", "key1c", "value1c"));
    RequiredTopic remainingTopic2 =
        random
            .nextObject(RequiredTopic.class)
            .withConfig(Map.of("key2a", "value2a", "key2b", "value2b", "key2c", "value2c"));

    RequiredTopic remainingTopicWithUpdatedConfig =
        remainingTopic2.withConfig(Map.of("key2b", "value2b", "key2c", "value2x"));

    Set<ExistingTopic> currentTopics = new HashSet<>();
    currentTopics.add(toExisting(topicToDelete));
    currentTopics.add(toExisting(remainingTopic1));
    currentTopics.add(toExisting(remainingTopic2));

    Set<RequiredTopic> requiredTopics = new HashSet<>();
    requiredTopics.add(topicToCreate);
    requiredTopics.add(remainingTopic1);
    requiredTopics.add(remainingTopicWithUpdatedConfig);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(currentTopics), requiredState.withTopics(requiredTopics));

    Map<TopicName, Map<String, Optional<String>>> result = deltaCalculator.topicConfigUpdate();

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

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(currentState.withBrokers(toBroker(allBrokers)), requiredState);
    List<Integer> result = deltaCalculator.selectBrokersForReplication(currentBrokers, 7);

    assertThat(result).hasSize(7);
    List<Integer> addedBrokers = new ArrayList<>();
    addedBrokers.addAll(result);
    addedBrokers.removeAll(currentBrokers);
    assertThat(addedBrokers).hasSize(3);
    assertThat(availableBrokers).containsAll(addedBrokers);
  }

  @Test
  public void whenReplicationFactorIsLowered_unneededBrokersAreRemoved() {
    List<Integer> utilizedBrokers = List.of(1, 2, 3, 4, 5, 6, 7);

    DeltaCalculator deltaCalculator = new DeltaCalculator(currentState, requiredState);
    List<Integer> result = deltaCalculator.selectBrokersForReplication(utilizedBrokers, 4);

    assertThat(result).hasSize(4);
    assertThat(utilizedBrokers).containsAll(result);
  }

  @Test
  public void whenReplicationFactorIsUnchanged_currentStateIsReturned() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(currentState.withBrokers(toBroker(currentBrokers)), requiredState);
    List<Integer> result =
        deltaCalculator.selectBrokersForReplication(currentBrokers, currentBrokers.size());

    assertThat(result).containsExactlyInAnyOrderElementsOf(currentBrokers);
  }

  @Test
  public void whenCurrentBrokersIsNotSubsetOfAllBrokers_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 14);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(currentState.withBrokers(createBrokers(12)), requiredState);
    assertThatThrownBy(
            () ->
                deltaCalculator.selectBrokersForReplication(currentBrokers, currentBrokers.size()))
        .hasMessageContaining("Invalid replication state");
  }

  @Test
  public void whenReplicationFactorIsLessThanOne_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(currentState.withBrokers(createBrokers(12)), requiredState);
    assertThatThrownBy(() -> deltaCalculator.selectBrokersForReplication(currentBrokers, 0))
        .hasMessage("Replication factor must be greater than 0");
  }

  @Test
  public void whenReplicationFactorGreaterThanAllBrokers_exceptionThrown() {
    List<Integer> currentBrokers = List.of(2, 3, 5, 6);
    List<Broker> allBrokers = createBrokers(12);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(currentState.withBrokers(allBrokers), requiredState);
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
    Set<ExistingTopic> currentTopics =
        Set.of(
            new ExistingTopic(
                TopicName.of("topic-1"), Map.of(PartitionNumber.of(0), List.of(0)), Map.of()),
            new ExistingTopic(
                TopicName.of("topic-2"), Map.of(PartitionNumber.of(0), List.of(0)), Map.of()),
            new ExistingTopic(
                TopicName.of("topic-3"), Map.of(PartitionNumber.of(0), List.of(0)), Map.of()));

    Set<RequiredTopic> requiredTopics =
        Set.of(
            new RequiredTopic(TopicName.of("topic-1"), Optional.of(1), Optional.of(1), Map.of()),
            new RequiredTopic(TopicName.of("topic-2"), Optional.of(4), Optional.of(1), Map.of()),
            new RequiredTopic(TopicName.of("topic-3"), Optional.of(2), Optional.of(1), Map.of()));

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(currentTopics), requiredState.withTopics(requiredTopics));
    Map<TopicName, Integer> result = deltaCalculator.partitionUpdate();

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TopicName.of("topic-2"), 4,
                TopicName.of("topic-3"), 2));
  }

  @Test
  public void whenPartitionCountDecreased_errorThrown() {
    Set<ExistingTopic> currentTopics =
        Set.of(
            new ExistingTopic(
                TopicName.of("topic-1"), Map.of(PartitionNumber.of(0), List.of(0)), Map.of()),
            new ExistingTopic(
                TopicName.of("topic-2"), Map.of(PartitionNumber.of(0), List.of(0)), Map.of()),
            new ExistingTopic(
                TopicName.of("topic-3"),
                Map.of(
                    PartitionNumber.of(0), List.of(0),
                    PartitionNumber.of(1), List.of(0),
                    PartitionNumber.of(2), List.of(0)),
                Map.of()));

    Set<RequiredTopic> requiredTopics =
        Set.of(
            new RequiredTopic(TopicName.of("topic-1"), Optional.of(1), Optional.of(1), Map.of()),
            new RequiredTopic(TopicName.of("topic-2"), Optional.of(4), Optional.of(1), Map.of()),
            new RequiredTopic(TopicName.of("topic-3"), Optional.of(2), Optional.of(1), Map.of()));

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(currentTopics), requiredState.withTopics(requiredTopics));
    assertThatThrownBy(deltaCalculator::partitionUpdate)
        .hasMessage(
            "Number of partitions cannot be lowered. Current number of partitions: 3, requested number of partitions: 2");
  }

  @Test
  public void testReplication() {
    Set<ExistingTopic> currentTopics =
        Set.of(
            new ExistingTopic(
                TopicName.of("topic-1"),
                Map.of(PartitionNumber.of(0), List.of(1, 3, 4), PartitionNumber.of(1), List.of(3)),
                Map.of()),
            new ExistingTopic(
                TopicName.of("topic-3"),
                Map.of(PartitionNumber.of(0), List.of(1, 2, 3)),
                Map.of()));

    Set<RequiredTopic> requiredTopics =
        Set.of(
            new RequiredTopic(TopicName.of("topic-1"), Optional.of(2), Optional.of(3), Map.of()),
            new RequiredTopic(TopicName.of("topic-3"), Optional.of(1), Optional.of(2), Map.of()));

    class MockDeltaCalculator extends DeltaCalculator {
      public MockDeltaCalculator(CurrentState currentState, RequiredState requiredState) {
        super(currentState, requiredState);
      }

      @Override
      public List<Integer> selectBrokersForReplication(
          Collection<Integer> utilizedBrokers, int replicationFactor) {
        return Map.of(
                List.of(1, 3, 4), List.of(1, 3, 4),
                List.of(3), List.of(3, 5, 6),
                List.of(1, 2, 3), List.of(1, 2))
            .get(List.copyOf(utilizedBrokers));
      }
    }

    CurrentState currentState = new CurrentState(currentTopics, Set.of(), Set.of());
    RequiredState requiredState = this.requiredState.withTopics(requiredTopics);
    MockDeltaCalculator deltaCalculator = new MockDeltaCalculator(currentState, requiredState);

    Map<TopicName, List<Partition>> result = deltaCalculator.replicationUpdate();

    assertThat(result)
        .containsExactlyInAnyOrderEntriesOf(
            Map.of(
                TopicName.of("topic-1"),
                    List.of(new Partition(PartitionNumber.of(1), List.of(3, 5, 6))),
                TopicName.of("topic-3"),
                    List.of(new Partition(PartitionNumber.of(0), List.of(1, 2)))));
  }

  @Test
  public void givenBrokerConfiguration_whenConfigRemoved_brokerIsSetToDefault() {
    Broker broker =
        new Broker(
            BrokerId.of("1"),
            Map.of(
                "key1", new BrokerConfig("key1", "value1", false, false),
                "key2", new BrokerConfig("key2", "value2", false, false),
                "key3", new BrokerConfig("key3", "value3", false, false)));

    Map<String, String> requiredBrokerConfig =
        Map.of(
            "key1", "value1",
            "key3", "value3-new");

    var result =
        new DeltaCalculator(
                currentState.withBrokers(Set.of(broker)),
                requiredState.withBrokers(requiredBrokerConfig))
            .brokerConfigUpdate();

    assertThat(result)
        .isEqualTo(
            Map.of(
                BrokerId.of("1"),
                Map.of(
                    "key2", Optional.empty(),
                    "key3", Optional.of("value3-new"))));
  }

  private List<ExistingTopic> toExistingAll(List<RequiredTopic> topics) {
    return topics.stream().map(this::toExisting).collect(Collectors.toList());
  }

  private ExistingTopic toExisting(RequiredTopic topic) {
    Map<PartitionNumber, Collection<Integer>> partitions =
        Stream.range(0, topic.getPartitionCount().orElse(1))
            .toJavaMap(
                no ->
                    Tuple.of(
                        PartitionNumber.of(no),
                        Stream.range(0, topic.getReplicationFactor().orElse(1))
                            .map(Integer::valueOf)
                            .toJavaList()));

    return new ExistingTopic(topic.getName(), partitions, topic.getConfig());
  }
}
