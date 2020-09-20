package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import org.jeasy.random.EasyRandom;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


public class DeltaCalculatorTest {

    EasyRandom random = TestUtil.randomizer;
    DeltaCalculator deltaCalculator = new DeltaCalculator();

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

        Set<Topic> result = deltaCalculator.topicsToCreate(toExistingAll(existingTopics), existingTopics);

        assertThat(result).isEmpty();
    }

    @Test
    public void whenNoTopic_returnEmptySet() {
        assertThat(deltaCalculator.topicsToCreate(Set.of(), Set.of())).isEmpty();
        assertThat(deltaCalculator.topicsToDelete(Set.of(), Set.of())).isEmpty();
    }

    @Test
    public void findTopicsToDelete() {
        Set<Topic> requiredTopics = random.objects(Topic.class, 10).collect(Collectors.toSet());
        Set<ExistingTopic> oldTopics = random.objects(ExistingTopic.class, 5).collect(Collectors.toSet());

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

        Topic remainingTopic1 = random.nextObject(Topic.class).withConfig(Map.of("key1a", "value1a", "key1b", "value1b", "key1c", "value1c"));
        Topic remainingTopic2 = random.nextObject(Topic.class).withConfig(Map.of("key2a", "value2a", "key2b", "value2b", "key2c", "value2c"));

        Topic remainingTopicWithUpdatedConfig = remainingTopic2.withConfig(Map.of("key2b", "value2b", "key2c", "value2x"));

        Set<ExistingTopic> currentState = new HashSet<>();
        currentState.add(toExisting(topicToDelete));
        currentState.add(toExisting(remainingTopic1));
        currentState.add(toExisting(remainingTopic2));

        Set<Topic> requiredState = new HashSet<>();
        requiredState.add(topicToCreate);
        requiredState.add(remainingTopic1);
        requiredState.add(remainingTopicWithUpdatedConfig);

        Map<String, Map<String, Optional<String>>> result = deltaCalculator.topicConfigUpdate(currentState, requiredState);

        assertThat(result.get(remainingTopicWithUpdatedConfig.getName())).containsExactlyInAnyOrderEntriesOf(Map.of(
            "key2a", Optional.empty(),
            "key2c", Optional.of("value2x")
        ));
    }

    @Test
    public void whenReplicationFactorIsIncreased_extraBrokersAreAdded() {
        List<Broker> availableBrokers = List.of(
            new Broker(0),
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5));
        List<Broker> currentBrokers = List.of(
            new Broker(6),
            new Broker(7),
            new Broker(8),
            new Broker(9));

        List<Broker> allBrokers = new ArrayList<>();
        allBrokers.addAll(availableBrokers);
        allBrokers.addAll(currentBrokers);

        List<Broker> result = deltaCalculator.selectBrokersForReplication(
            allBrokers,
            currentBrokers,
            7);

        assertThat(result).hasSize(7);
        List<Broker> addedBrokers = new ArrayList<>();
        addedBrokers.addAll(result);
        addedBrokers.removeAll(currentBrokers);
        assertThat(addedBrokers).hasSize(3);
        assertThat(availableBrokers).containsAll(addedBrokers);
    }

    @Test
    public void whenReplicationFactorIsLowered_unneededBrokersAreRemoved() {
        List<Broker> allBrokers = List.of(
            new Broker(0),
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6),
            new Broker(7),
            new Broker(8),
            new Broker(9),
            new Broker(10),
            new Broker(11),
            new Broker(12));

        List<Broker> currentBrokers = List.of(
            new Broker(0),
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6),
            new Broker(7)
        );

        List<Broker> result = deltaCalculator.selectBrokersForReplication(
            allBrokers,
            currentBrokers,
            4);

        assertThat(result).hasSize(4);
        assertThat(currentBrokers).containsAll(result);
    }

    @Test
    public void whenReplicationFactorIsUnchanged_currentStateIsReturned() {
        List<Broker> allBrokers = List.of(
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6)
        );
        List<Broker> currentBrokers = List.of(
            new Broker(2),
            new Broker(3),
            new Broker(5),
            new Broker(6)
        );
        List<Broker> result = deltaCalculator.selectBrokersForReplication(
            allBrokers,
            currentBrokers,
            currentBrokers.size());

        assertThat(result).containsExactlyInAnyOrderElementsOf(currentBrokers);
    }

    @Test
    public void whenCurrentBrokersIsNotSubsetOfAllBrokers_exceptionThrown() {
        List<Broker> allBrokers = List.of(
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6)
        );
        List<Broker> currentBrokers = List.of(
            new Broker(2),
            new Broker(3),
            new Broker(5),
            new Broker(7)
        );

        assertThatThrownBy(() -> deltaCalculator.selectBrokersForReplication(allBrokers, currentBrokers, currentBrokers.size()))
            .hasMessage("Invalid replication state - All Brokers: [Model.Broker(id=1), Model.Broker(id=2), Model.Broker(id=3), Model.Broker(id=4), Model.Broker(id=5), Model.Broker(id=6)], Used Brokers: [Model.Broker(id=2), Model.Broker(id=3), Model.Broker(id=5), Model.Broker(id=7)]");
    }

    @Test
    public void whenReplicationFactorIsLessThanOne_exceptionThrown() {
        List<Broker> allBrokers = List.of(
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6)
        );
        List<Broker> currentBrokers = List.of(
            new Broker(2),
            new Broker(3),
            new Broker(5),
            new Broker(6)
        );

        assertThatThrownBy(() -> deltaCalculator.selectBrokersForReplication(allBrokers, currentBrokers, 0))
            .hasMessage("Replication factor must be greater than 0");
    }

    @Test
    public void whenReplicationFactorGreaterThanAllBrokers_exceptionThrown() {
        List<Broker> allBrokers = List.of(
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6)
        );
        List<Broker> currentBrokers = List.of(
            new Broker(2),
            new Broker(3),
            new Broker(5),
            new Broker(6)
        );

        assertThatThrownBy(() -> deltaCalculator.selectBrokersForReplication(allBrokers, currentBrokers, allBrokers.size() + 1))
            .hasMessage(String.format("Replication factor [%d] must not be greater than the number of brokers [%d]", allBrokers.size() + 1, allBrokers.size()));
    }

    @Test
    public void whenPartitionCountIncreased_topicWithNewPartitionCountIncluded() {
        Set<ExistingTopic> currentState = Set.of(
            new ExistingTopic("topic-1", Set.of(new Partition(0, List.of(new Broker(0)))), Map.of()),
            new ExistingTopic("topic-2", Set.of(new Partition(0, List.of(new Broker(0)))), Map.of()),
            new ExistingTopic("topic-3", Set.of(new Partition(0, List.of(new Broker(0)))), Map.of())
        );

        Set<Topic> requiredState = Set.of(
            new Topic("topic-1", 1, 1, Map.of()),
            new Topic("topic-2", 4, 1, Map.of()),
            new Topic("topic-3", 2, 1, Map.of())
        );

        Map<String, Integer> result = deltaCalculator.partitionUpdate(
            currentState,
            requiredState
        );

        assertThat(result).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "topic-2", 4,
                "topic-3", 2
            ));
    }

    @Test
    public void whenPartitionCountDecreased_errorThrown() {
        Set<ExistingTopic> currentState = Set.of(
            new ExistingTopic("topic-1", Set.of(new Partition(0, List.of(new Broker(0)))), Map.of()),
            new ExistingTopic("topic-2", Set.of(new Partition(0, List.of(new Broker(0)))), Map.of()),
            new ExistingTopic(
                "topic-3", Set.of(
                    new Partition(0, List.of(new Broker(0))),
                    new Partition(1, List.of(new Broker(0))),
                    new Partition(2, List.of(new Broker(0)))),
                Map.of())
        );

        Set<Topic> requiredState = Set.of(
            new Topic("topic-1", 1, 1, Map.of()),
            new Topic("topic-2", 4, 1, Map.of()),
            new Topic("topic-3", 2, 1, Map.of())
        );

        assertThatThrownBy(() -> deltaCalculator.partitionUpdate(currentState, requiredState))
            .hasMessage("Number of partitions cannot be lowered. Current number of partitions: 3, requested number of partitions: 2");
    }

    @Test
    public void testReplication() {
        Set<ExistingTopic> currentState = Set.of(
            new ExistingTopic("topic-1", Set.of(
                new Partition(0, List.of(new Broker(1), new Broker(3), new Broker(4))),
                new Partition(1, List.of(new Broker(3)))), Map.of()),
            new ExistingTopic("topic-3", Set.of(new Partition(0, List.of(new Broker(1), new Broker(2), new Broker(3)))), Map.of())
        );

        Set<Topic> requiredState = Set.of(
            new Topic("topic-1", 2, 3, Map.of()),
            new Topic("topic-3", 1, 2, Map.of())
        );

        List<Broker> allBrokers = List.of(
            new Broker(1),
            new Broker(2),
            new Broker(3),
            new Broker(4),
            new Broker(5),
            new Broker(6)
        );

        DeltaCalculator deltaCalculator = mock(DeltaCalculator.class);
        when(deltaCalculator.selectBrokersForReplication(any(), eq(List.of(new Broker(1), new Broker(3), new Broker(4))), eq(3))).thenReturn(List.of(new Broker(1), new Broker(3), new Broker(4)));
        when(deltaCalculator.selectBrokersForReplication(any(), eq(List.of(new Broker(3))), eq(3))).thenReturn(List.of(new Broker(3), new Broker(5), new Broker(6)));
        when(deltaCalculator.selectBrokersForReplication(any(), eq(List.of(new Broker(1), new Broker(2), new Broker(3))), eq(2))).thenReturn(List.of(new Broker(1), new Broker(2)));
        when(deltaCalculator.replicationUpdate(currentState, requiredState, Set.copyOf(allBrokers))).thenCallRealMethod();

        Map<String, Collection<Partition>> result = deltaCalculator.replicationUpdate(currentState, requiredState, Set.copyOf(allBrokers));

        assertThat(result).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                "topic-1", Set.of(new Partition(1, List.of(new Broker(3), new Broker(5), new Broker(6)))),
                "topic-3", Set.of(new Partition(0, List.of(new Broker(1), new Broker(2))))
            )
        );
    }

    private Set<ExistingTopic> toExistingAll(Set<Topic> topics) {
        return topics.stream().map(this::toExisting).collect(Collectors.toSet());
    }

    private ExistingTopic toExisting(Topic topic) {
        Set<Partition> partitions = IntStream
            .range(0, topic.getPartitionCount())
            .mapToObj(no -> new Partition(no, IntStream.range(0, topic.getReplicationFactor()).mapToObj(Broker::new).collect(Collectors.toList())))
            .collect(Collectors.toSet());

        return new ExistingTopic(
            topic.getName(),
            partitions,
            topic.getConfig()
        );
    }
}
