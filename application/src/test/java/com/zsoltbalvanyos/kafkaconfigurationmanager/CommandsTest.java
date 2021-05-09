package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.times;

import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.resource.ResourceType;
import org.jeasy.random.EasyRandom;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class CommandsTest {

  EasyRandom random = TestUtil.randomizer();

  @Mock KafkaFuture<Void> kafkaFuture;

  Admin admin = mock(Admin.class);
  Mappers mappers = new Mappers();
  @InjectMocks Commands commands = new Commands(admin, mappers);

  @Test
  public void updateRequestBuild() {
    Map<TopicName, Map<String, Optional<String>>> update =
        HashMap.of(
            TopicName.of("topic1"),
            HashMap.of(
                "config1", Optional.of("value1"),
                "config2", Optional.of("value2")),
            TopicName.of("topic2"),
            HashMap.of(
                "config3", Optional.empty(),
                "config4", Optional.of("value4")));

    Map<ConfigResource, Collection<AlterConfigOp>> result =
        mappers.getAlterConfigRequest(update, ConfigResource.Type.TOPIC);

    assertThat(result)
        .containsExactlyInAnyOrderElementsOf(
            HashMap.of(
                new ConfigResource(ConfigResource.Type.TOPIC, "topic1"),
                List.of(
                    new AlterConfigOp(
                        new ConfigEntry("config1", "value1"), AlterConfigOp.OpType.SET),
                    new AlterConfigOp(
                        new ConfigEntry("config2", "value2"), AlterConfigOp.OpType.SET)),
                new ConfigResource(ConfigResource.Type.TOPIC, "topic2"),
                List.of(
                    new AlterConfigOp(new ConfigEntry("config3", ""), AlterConfigOp.OpType.DELETE),
                    new AlterConfigOp(
                        new ConfigEntry("config4", "value4"), AlterConfigOp.OpType.SET))));
  }

  @Test
  public void givenListOfTopics_whenCreateTopicsCalled_thenAdminCreateTopicsCalled() {
    var topics = getRandom(RequiredTopic.class, 10);
    var newTopics = topics.map(mappers::toNewTopic);

    CreateTopicsResult createTopicsResult = mock(CreateTopicsResult.class);
    when(createTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.createTopics(eq(newTopics.toJavaSet()))).thenReturn(createTopicsResult);

    commands.createTopics(HashSet.ofAll(topics));
    verify(admin, times(1)).createTopics(eq(newTopics.toJavaSet()));
  }

  @Test
  public void givenListOfTopicNames_whenDeleteTopicsCalled_thenAdminDeleteTopicsCalled() {
    var topicNames = getRandom(String.class, 10);

    DeleteTopicsResult deleteTopicsResult = mock(DeleteTopicsResult.class);
    when(deleteTopicsResult.all()).thenReturn(kafkaFuture);
    when(admin.deleteTopics(eq(topicNames.toJavaSet()))).thenReturn(deleteTopicsResult);

    commands.deleteTopics(topicNames);
    verify(admin, times(1)).deleteTopics(topicNames.toJavaSet());
  }

  @Test
  public void givenAcls_whenCreateAcls_thenAdminCreateAclsCalled() {
    var acls =
        HashSet.ofAll(
            random
                .objects(Acl.class, 10)
                .map(a -> a.withResourceType(ResourceType.TOPIC.name()))
                .collect(Collectors.toSet()));
    var newAcls = acls.flatMap(mappers::toAclBinding);

    CreateAclsResult createAclsResult = mock(CreateAclsResult.class);
    when(createAclsResult.all()).thenReturn(kafkaFuture);
    when(admin.createAcls(eq(newAcls.toJavaSet()))).thenReturn(createAclsResult);

    commands.createAcls(acls);
    verify(admin, times(1)).createAcls(eq(newAcls.toJavaSet()));
  }

  @Test
  public void givenAcls_whenDeleteAcls_thenAdminDeleteAclsCalled() {
    var acls = getRandom(Acl.class, 10).map(a -> a.withResourceType(ResourceType.TOPIC.name()));
    var aclsToDelete = acls.flatMap(mappers::toAclBindingFilter);

    AclBinding aclBinding = mock(AclBinding.class);
    DeleteAclsResult deleteAclsResult = mock(DeleteAclsResult.class);
    when(deleteAclsResult.all()).thenReturn(KafkaFuture.completedFuture(List.of(aclBinding)));
    when(admin.deleteAcls(eq(aclsToDelete.toJavaSet()))).thenReturn(deleteAclsResult);

    commands.deleteAcls(acls);
    verify(admin, times(1)).deleteAcls(eq(aclsToDelete.toJavaSet()));
  }

  private <T> io.vavr.collection.List<T> getRandom(Class<T> clazz, int n) {
    return io.vavr.collection.List.ofAll(random.objects(clazz, n).collect(Collectors.toList()));
  }
}
