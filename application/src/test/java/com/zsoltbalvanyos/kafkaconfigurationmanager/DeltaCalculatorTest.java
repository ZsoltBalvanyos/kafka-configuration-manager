package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import io.vavr.Tuple;
import io.vavr.collection.*;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.jeasy.random.EasyRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class DeltaCalculatorTest {

  CurrentState currentState = new CurrentState(HashSet.empty(), createBrokers(12), HashSet.empty());
  RequiredState requiredState =
      new RequiredState(HashSet.empty(), HashMap.empty(), HashSet.empty());

  EasyRandom random = TestUtil.randomizer();
  DeltaCalculator deltaCalculator = new DeltaCalculator(currentState, requiredState);

  private List<Broker> createBrokers(int n) {
    return List.ofAll(
        IntStream.range(0, n)
            .mapToObj(i -> new Broker(BrokerId.of(String.valueOf(i + 1)), HashMap.empty())));
  }

  private Traversable<Broker> toBroker(Traversable<Integer> n) {
    return n.map(String::valueOf).map(i -> new Broker(BrokerId.of(i), HashMap.empty()));
  }

  @Test
  public void findNewAcls() {
    List<Acl> existingAcls = getRandom(Acl.class, 10);
    List<Acl> newAcls = getRandom(Acl.class, 5);
    List<Acl> merged = existingAcls.appendAll(newAcls);

    var deltaCalculator =
        new DeltaCalculator(
            currentState.withAcls(List.ofAll(existingAcls)),
            requiredState.withAcls(List.ofAll(merged)));
    java.util.List<Acl> result = deltaCalculator.aclsToCreate().toJavaList();

    assertThat(result).containsExactlyInAnyOrderElementsOf(newAcls);
  }

  @Test
  public void whenNoNewAcl_returnEmptySet() {
    List<Acl> existingAcls = getRandom(Acl.class, 10);

    var deltaCalculator = new DeltaCalculator(currentState.withAcls(existingAcls), requiredState);
    Traversable<Acl> result = deltaCalculator.aclsToCreate();

    assertThat(result).isEmpty();
  }

  @Test
  public void findAclsToDelete() {
    List<Acl> requiredAcls = getRandom(Acl.class, 10);
    List<Acl> oldAcls = getRandom(Acl.class, 5);

    List<Acl> merged = List.ofAll(oldAcls);
    merged.appendAll(requiredAcls);

    var deltaCalculator =
        new DeltaCalculator(currentState.withAcls(merged), requiredState.withAcls(requiredAcls));
    Traversable<Acl> result = deltaCalculator.aclsToDelete();

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldAcls);
  }

  @Test
  public void whenNoAclToDelete_returnEmptySet() {
    List<Acl> acls = getRandom(Acl.class, 10);

    var deltaCalculator =
        new DeltaCalculator(currentState.withAcls(acls), requiredState.withAcls(acls));
    Traversable<Acl> result = deltaCalculator.aclsToDelete();

    assertThat(result).isEmpty();
  }

  @Test
  public void findNewTopics() {
    List<RequiredTopic> existingTopics = getRandom(RequiredTopic.class, 10);
    List<RequiredTopic> newTopics = getRandom(RequiredTopic.class, 5);
    List<RequiredTopic> merged = List.ofAll(newTopics);
    merged.appendAll(existingTopics);

    var deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(toExistingAll(existingTopics)),
            requiredState.withTopics(merged));
    Traversable<RequiredTopic> result = deltaCalculator.topicsToCreate();

    assertThat(result).containsExactlyInAnyOrderElementsOf(newTopics);
  }

  @Test
  public void whenNoNewTopic_returnEmptySet() {
    List<RequiredTopic> requiredTopics = getRandom(RequiredTopic.class, 10);

    var deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(toExistingAll(requiredTopics)),
            requiredState.withTopics(requiredTopics));
    Traversable<RequiredTopic> result = deltaCalculator.topicsToCreate();

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
    List<RequiredTopic> requiredTopics = getRandom(RequiredTopic.class, 10);
    List<ExistingTopic> oldTopics = getRandom(ExistingTopic.class, 5);

    List<ExistingTopic> merged = List.ofAll(oldTopics);
    merged.appendAll(toExistingAll(requiredTopics));

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(merged), requiredState.withTopics(requiredTopics));
    Traversable<ExistingTopic> result = deltaCalculator.topicsToDelete();

    assertThat(result).containsExactlyInAnyOrderElementsOf(oldTopics);
  }

  @Test
  public void whenNoTopicToDelete_returnEmptyList() {
    List<RequiredTopic> topics = getRandom(RequiredTopic.class, 10);
    List<ExistingTopic> existingTopics = toExistingAll(topics);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(existingTopics), requiredState.withTopics(topics));
    Traversable<ExistingTopic> result = deltaCalculator.topicsToDelete();

    assertThat(result).isEmpty();
  }

  @Test
  public void testTopicConfigurationUpdate() {

    RequiredTopic topicToCreate = random.nextObject(RequiredTopic.class);
    RequiredTopic topicToDelete = random.nextObject(RequiredTopic.class);

    RequiredTopic remainingTopic1 =
        random
            .nextObject(RequiredTopic.class)
            .withConfig(HashMap.of("key1a", "value1a", "key1b", "value1b", "key1c", "value1c"));
    RequiredTopic remainingTopic2 =
        random
            .nextObject(RequiredTopic.class)
            .withConfig(HashMap.of("key2a", "value2a", "key2b", "value2b", "key2c", "value2c"));

    RequiredTopic remainingTopicWithUpdatedConfig =
        remainingTopic2.withConfig(HashMap.of("key2b", "value2b", "key2c", "value2x"));

    Set<ExistingTopic> currentTopics =
        HashSet.of(
            toExisting(topicToDelete), toExisting(remainingTopic1), toExisting(remainingTopic2));

    Set<RequiredTopic> requiredTopics =
        HashSet.of(topicToCreate, remainingTopic1, remainingTopicWithUpdatedConfig);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(currentTopics), requiredState.withTopics(requiredTopics));

    Map<TopicName, Map<String, Optional<String>>> result = deltaCalculator.topicConfigUpdate();

    assertThat(result.get(remainingTopicWithUpdatedConfig.getName()).get())
        .containsExactlyInAnyOrderElementsOf(
            HashMap.of(
                "key2a", Optional.empty(),
                "key2c", Optional.of("value2x")));
  }

  @Test
  public void whenReplicationFactorIsIncreased_extraBrokersAreAdded() {
    List<Integer> availableBrokers = List.of(0, 1, 2, 3, 4, 5);
    List<Integer> currentBrokers = List.of(6, 7, 8, 9);

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withBrokers(toBroker(availableBrokers.appendAll(currentBrokers))),
            requiredState);
    List<Integer> result = deltaCalculator.selectBrokersForReplication(currentBrokers, 7);

    assertThat(result).hasSize(7);
    List<Integer> addedBrokers = result.removeAll(currentBrokers);

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
        HashSet.of(
            new ExistingTopic(
                TopicName.of("topic-1"),
                HashMap.of(PartitionNumber.of(0), List.of(0)),
                HashMap.empty()),
            new ExistingTopic(
                TopicName.of("topic-2"),
                HashMap.of(PartitionNumber.of(0), List.of(0)),
                HashMap.empty()),
            new ExistingTopic(
                TopicName.of("topic-3"),
                HashMap.of(PartitionNumber.of(0), List.of(0)),
                HashMap.empty()));

    Set<RequiredTopic> requiredTopics =
        HashSet.of(
            new RequiredTopic(
                TopicName.of("topic-1"), Optional.of(1), Optional.of(1), HashMap.empty()),
            new RequiredTopic(
                TopicName.of("topic-2"), Optional.of(4), Optional.of(1), HashMap.empty()),
            new RequiredTopic(
                TopicName.of("topic-3"), Optional.of(2), Optional.of(1), HashMap.empty()));

    DeltaCalculator deltaCalculator =
        new DeltaCalculator(
            currentState.withTopics(currentTopics), requiredState.withTopics(requiredTopics));
    Map<TopicName, Integer> result = deltaCalculator.partitionUpdate();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            HashMap.of(
                TopicName.of("topic-2"), 4,
                TopicName.of("topic-3"), 2));
  }

  @Test
  public void whenPartitionCountDecreased_errorThrown() {
    Set<ExistingTopic> currentTopics =
        HashSet.of(
            new ExistingTopic(
                TopicName.of("topic-1"),
                HashMap.of(PartitionNumber.of(0), List.of(0)),
                HashMap.empty()),
            new ExistingTopic(
                TopicName.of("topic-2"),
                HashMap.of(PartitionNumber.of(0), List.of(0)),
                HashMap.empty()),
            new ExistingTopic(
                TopicName.of("topic-3"),
                HashMap.of(
                    PartitionNumber.of(0), List.of(0),
                    PartitionNumber.of(1), List.of(0),
                    PartitionNumber.of(2), List.of(0)),
                HashMap.empty()));

    Set<RequiredTopic> requiredTopics =
        HashSet.of(
            new RequiredTopic(
                TopicName.of("topic-1"), Optional.of(1), Optional.of(1), HashMap.empty()),
            new RequiredTopic(
                TopicName.of("topic-2"), Optional.of(4), Optional.of(1), HashMap.empty()),
            new RequiredTopic(
                TopicName.of("topic-3"), Optional.of(2), Optional.of(1), HashMap.empty()));

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
        HashSet.of(
            new ExistingTopic(
                TopicName.of("topic-1"),
                HashMap.of(
                    PartitionNumber.of(0), List.of(1, 3, 4), PartitionNumber.of(1), List.of(3)),
                HashMap.empty()),
            new ExistingTopic(
                TopicName.of("topic-3"),
                HashMap.of(PartitionNumber.of(0), List.of(1, 2, 3)),
                HashMap.empty()));

    Set<RequiredTopic> requiredTopics =
        HashSet.of(
            new RequiredTopic(
                TopicName.of("topic-1"), Optional.of(2), Optional.of(3), HashMap.empty()),
            new RequiredTopic(
                TopicName.of("topic-3"), Optional.of(1), Optional.of(2), HashMap.empty()));

    class MockDeltaCalculator extends DeltaCalculator {
      public MockDeltaCalculator(CurrentState currentState, RequiredState requiredState) {
        super(currentState, requiredState);
      }

      @Override
      public List<Integer> selectBrokersForReplication(
          Traversable<Integer> utilizedBrokers, int replicationFactor) {
        return HashMap.of(
                List.of(1, 3, 4), List.of(1, 3, 4),
                List.of(3), List.of(3, 5, 6),
                List.of(1, 2, 3), List.of(1, 2))
            .get(List.ofAll(utilizedBrokers))
            .get();
      }
    }

    CurrentState currentState = new CurrentState(currentTopics, HashSet.empty(), HashSet.empty());
    RequiredState requiredState = this.requiredState.withTopics(requiredTopics);
    MockDeltaCalculator deltaCalculator = new MockDeltaCalculator(currentState, requiredState);

    Map<TopicName, Traversable<Partition>> result = deltaCalculator.replicationUpdate();

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            HashMap.of(
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
            HashMap.of(
                "key1", new BrokerConfig("key1", "value1", false, false),
                "key2", new BrokerConfig("key2", "value2", false, false),
                "key3", new BrokerConfig("key3", "value3", false, false)));

    Map<String, String> requiredBrokerConfig =
        HashMap.of(
            "key1", "value1",
            "key3", "value3-new");

    var result =
        new DeltaCalculator(
                currentState.withBrokers(HashSet.of(broker)),
                requiredState.withBrokers(requiredBrokerConfig))
            .brokerConfigUpdate();

    assertThat(result)
        .isEqualTo(
            HashMap.of(
                BrokerId.of("1"),
                HashMap.of(
                    "key2", Optional.empty(),
                    "key3", Optional.of("value3-new"))));
  }

  private List<ExistingTopic> toExistingAll(List<RequiredTopic> topics) {
    return topics.map(this::toExisting);
  }

  private ExistingTopic toExisting(RequiredTopic topic) {
    Map<PartitionNumber, Traversable<Integer>> partitions =
        Stream.range(0, topic.getPartitionCount().orElse(1))
            .toMap(
                no ->
                    Tuple.of(
                        PartitionNumber.of(no),
                        Stream.range(0, topic.getReplicationFactor().orElse(1))
                            .map(Integer::valueOf)));

    return new ExistingTopic(topic.getName(), partitions, topic.getConfig());
  }

  private <T> List<T> getRandom(Class<T> clazz, int n) {
    return List.ofAll(random.objects(clazz, n).collect(Collectors.toList()));
  }
}
