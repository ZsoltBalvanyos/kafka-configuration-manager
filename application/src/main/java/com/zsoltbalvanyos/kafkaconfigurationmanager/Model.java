package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import io.vavr.collection.*;
import java.util.Optional;
import java.util.function.Function;
import lombok.*;

public class Model {

  @Value
  @With
  public static class Configuration {
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Map<String, String> brokerConfig;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Map<String, String>> topics;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Map<String, String>> configSets;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Acl> acls;
  }

  public interface Identified {
    Id<?> getName();
  }

  abstract static class Id<T extends Comparable<T>> {
    public T get() {
      return getValue();
    }

    abstract T getValue();

    public String toString() {
      return String.valueOf(getValue());
    }
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode(callSuper = false)
  public static class BrokerId extends Id<String> implements Comparable<BrokerId> {
    @Getter String value;

    @Override
    public int compareTo(BrokerId o) {
      return value.compareTo(o.value);
    }
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode(callSuper = false)
  public static class TopicName extends Id<String> implements Comparable<TopicName> {
    @Getter String value;

    @Override
    public int compareTo(TopicName o) {
      return value.compareTo(o.value);
    }
  }

  @AllArgsConstructor(staticName = "of")
  @EqualsAndHashCode(callSuper = false)
  public static class PartitionNumber extends Id<Integer> implements Comparable<PartitionNumber> {
    @Getter Integer value;

    @Override
    public int compareTo(PartitionNumber o) {
      return value.compareTo(o.value);
    }
  }

  @Value
  @With
  public static class ExistingTopic implements Identified {
    TopicName name;
    Map<PartitionNumber, Traversable<Integer>> partitions;
    Map<String, String> config;
  }

  @Value
  @With
  public static class RequiredTopic implements Identified {
    TopicName name;
    Optional<Integer> partitionCount;
    Optional<Integer> replicationFactor;
    Map<String, String> config;
  }

  @Value
  @With
  public static class Partition {
    PartitionNumber partitionNumber;
    List<Integer> replicas;
  }

  @Value
  @With
  public static class Broker {
    BrokerId id;
    Map<String, BrokerConfig> config;
  }

  @Value
  @With
  public static class BrokerConfig {
    String name;
    String value;
    boolean isDefault;
    boolean isReadOnly;
  }

  @Value
  @With
  public static class ExecutionPlan {
    Map<TopicName, Traversable<Partition>> replicationChanges;
    Map<TopicName, Integer> partitionChanges;
    Map<TopicName, Map<String, Optional<String>>> topicConfigurationChanges;
    Traversable<RequiredTopic> topicsToCreate;
    Traversable<ExistingTopic> topicsToDelete;
    Map<BrokerId, Map<String, Optional<String>>> brokerConfigurationChanges;
    Traversable<Acl> aclsToCreate;
    Traversable<Acl> aclsToDelete;
  }

  @Value
  @With
  static class Acl {
    String resourceType;
    String name;
    String patternType;
    Seq<Permission> permissions;
  }

  @Value
  @With
  static class Permission {
    String principal;
    String host;
    String operation;
    String permissionType;
  }

  @Value
  @With
  static class CurrentState {
    Traversable<ExistingTopic> topics;
    Traversable<Broker> brokers;
    Traversable<Acl> acls;

    @Getter(lazy = true)
    Map<TopicName, ExistingTopic> topicMap =
        topics.toMap(ExistingTopic::getName, Function.identity());
  }

  @Value
  @With
  static class RequiredState {
    Traversable<RequiredTopic> topics;
    Map<String, String> brokers;
    Traversable<Acl> acls;
  }
}
