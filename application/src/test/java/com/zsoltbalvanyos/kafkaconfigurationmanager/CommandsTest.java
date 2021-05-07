package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import java.util.*;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.ResourceType;
import org.jeasy.random.EasyRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest {

  EasyRandom random = TestUtil.randomizer;

  @Mock KafkaFuture<Void> kafkaFuture;

  Admin admin = mock(Admin.class);
  Mappers mappers = new Mappers();
  Commands commands = new Commands(admin, mappers);

  @Test
  public void updateRequestBuild() {
    Map<Model.TopicName, Map<String, Optional<String>>> update =
        Map.of(
            Model.TopicName.of("topic1"),
            Map.of(
                "config1", Optional.of("value1"),
                "config2", Optional.of("value2")),
            Model.TopicName.of("topic2"),
            Map.of(
                "config3", Optional.empty(),
                "config4", Optional.of("value4")));

    Map<ConfigResource, Collection<AlterConfigOp>> result =
        mappers.getAlterConfigRequest(update, ConfigResource.Type.TOPIC);

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
                    new AlterConfigOp(new ConfigEntry("config3", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(
                        new ConfigEntry("config4", "value4"), AlterConfigOp.OpType.SET))));
  }

  @Test
  public void givenListOfTopics_whenCreateTopicsCalled_thenAdminCreateTopicsCalled() {
    var topics = random.objects(Model.RequiredTopic.class, 10).collect(Collectors.toList());
    var newTopics = topics.stream().map(mappers::toNewTopic).collect(Collectors.toList());

    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    when(createTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.createTopics(eq(newTopics))).thenReturn(createTopicsResult);

    commands.createTopics(topics);
    verify(admin, times(1)).createTopics(eq(newTopics));
  }

  @Test
  public void givenListOfTopicNames_whenDeleteTopicsCalled_thenAdminDeleteTopicsCalled() {
    var topicNames = random.objects(String.class, 10).collect(Collectors.toList());

    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.deleteTopics(eq(topicNames))).thenReturn(deleteTopicsResult);

    commands.deleteTopics(topicNames);
    verify(admin, times(1)).deleteTopics(topicNames);
  }

  @Test
  public void givenAcls_whenCreateAcls_thenAdminCreateAclsCalled() {
    var acls =
        random
            .objects(Model.Acl.class, 10)
            .map(a -> a.withResourceType(ResourceType.TOPIC.name()))
            .collect(Collectors.toSet());
    var newAcls = acls.stream().flatMap(mappers::toAclBinding).collect(Collectors.toSet());

    CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
    when(createAclsResult.all()).thenReturn(kafkaFuture);
    when(admin.createAcls(eq(newAcls))).thenReturn(createAclsResult);

    commands.createAcls(acls);
    verify(admin, times(1)).createAcls(eq(newAcls));
  }

  @Test
  public void givenAcls_whenDeleteAcls_thenAdminDeleteAclsCalled() {
    var acls =
        random
            .objects(Model.Acl.class, 10)
            .map(a -> a.withResourceType(ResourceType.TOPIC.name()))
            .collect(Collectors.toSet());
    var aclsToDelete =
        acls.stream().flatMap(mappers::toAclBindingFilter).collect(Collectors.toSet());

    AclBinding aclBinding = mock(AclBinding.class);
    DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);
    when(deleteAclsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(aclBinding)));
    when(admin.deleteAcls(eq(aclsToDelete))).thenReturn(deleteAclsResult);

    commands.deleteAcls(acls);
    verify(admin, times(1)).deleteAcls(eq(aclsToDelete));
  }
}
