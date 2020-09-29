package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.jeasy.random.EasyRandom;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaClientTest {

    EasyRandom random = TestUtil.randomizer;

    Admin adminClient = mock(Admin.class);
    KafkaClient kafkaClient = new KafkaClient(adminClient);

    @Test
    public void existingTopicConfigsFetched() throws ExecutionException, InterruptedException {
        TopicListing topicListing1 = new TopicListing("topicListing1", false);
        TopicListing topicListing2 = new TopicListing("topicListing2", false);
        Config config1 = random.nextObject(Config.class);
        Config config2 = random.nextObject(Config.class);

        ListTopicsResult listTopicsResult = mock(ListTopicsResult.class);
        when(listTopicsResult.listings()).thenReturn(KafkaFuture.completedFuture(List.of(topicListing1, topicListing2)));

        when(adminClient.listTopics()).thenReturn(listTopicsResult);

        DescribeConfigsResult describeConfigsResult = mock(DescribeConfigsResult.class);
        when(describeConfigsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
            new ConfigResource(ConfigResource.Type.TOPIC, topicListing1.name()), config1,
            new ConfigResource(ConfigResource.Type.TOPIC, topicListing2.name()), config2
        )));
        when(adminClient.describeConfigs(any())).thenReturn(describeConfigsResult);

        DescribeTopicsResult describeTopicsResult = mock(DescribeTopicsResult.class);
        when(describeTopicsResult.all()).thenReturn(KafkaFuture.completedFuture(Map.of(
            topicListing1.name(), new TopicDescription(topicListing1.name(), false, List.of(new TopicPartitionInfo(0, null, List.of(new Node(0, "", 0)), List.of()))),
            topicListing2.name(), new TopicDescription(topicListing2.name(), false, List.of(new TopicPartitionInfo(0, null, List.of(new Node(0, "", 0)), List.of())))
        )));
        when(adminClient.describeTopics(any())).thenReturn(describeTopicsResult);

        Set<ExistingTopic> result = kafkaClient.getExistingTopics();

        assertThat(result).containsExactlyInAnyOrderElementsOf(
            Set.of(
                new ExistingTopic(
                    topicListing1.name(),
                    Set.of(new Partition(0, List.of(0))),
                    config1.entries().stream().filter(ce -> !ce.isDefault()).collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value))),
                new ExistingTopic(
                    topicListing2.name(),
                    Set.of(new Partition(0, List.of(0))),
                    config2.entries().stream().filter(ce -> !ce.isDefault()).collect(Collectors.toMap(ConfigEntry::name, ConfigEntry::value)))
            )
        );
    }

    @Test
    public void updateRequestBuild() {
        Map<String, Map<String, Optional<String>>> update = Map.of(
            "topic1", Map.of(
                "config1", Optional.of("value1"),
                "config2", Optional.of("value2")),
            "topic2", Map.of(
                "config3", Optional.empty(),
                "config4", Optional.of("value4"))
        );

        Map<ConfigResource, Collection<AlterConfigOp>> result = kafkaClient.getAlterConfigRequest(update);

        assertThat(result).containsExactlyInAnyOrderEntriesOf(
            Map.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "topic1"), Set.of(
                    new AlterConfigOp(new ConfigEntry("config1", "value1"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(new ConfigEntry("config2", "value2"), AlterConfigOp.OpType.SET)),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic2"), Set.of(
                    new AlterConfigOp(new ConfigEntry("config3", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(new ConfigEntry("config4", "value4"), AlterConfigOp.OpType.SET))
            )
        );
    }

}
