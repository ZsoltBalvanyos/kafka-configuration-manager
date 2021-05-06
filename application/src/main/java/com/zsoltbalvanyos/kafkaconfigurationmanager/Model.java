package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.*;
import lombok.Value;
import lombok.With;

public class Model {

  @Value
  @With
  public static class Configuration {
    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Map<String, String> brokerConfig;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Map<String, String>> perBrokerConfig;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Map<String, String>> topics;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Map<String, String>> configSets;

    @JsonSetter(nulls = Nulls.AS_EMPTY)
    Set<Acl> acls;
  }

  public interface Named {
    String getName();
  }

  @Value
  @With
  public static class ExistingTopic implements Named {
    String name;
    Collection<Partition> partitions;
    Map<String, String> config;
  }

  @Value
  @With
  public static class Topic implements Named {
    String name;
    Optional<Integer> partitionCount;
    Optional<Integer> replicationFactor;
    Map<String, String> config;
  }

  @Value
  @With
  public static class Partition {
    int partitionNumber;
    List<Integer> replicas;
  }

  @Value
  @With
  public static class Broker {
    int id;
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
    Map<String, Map<String, String>> originalConfigs;
    Map<String, Map<Integer, Integer>> originalPartitions;
    Map<String, List<Partition>> replicationChanges;
    Map<String, Integer> partitionChanges;
    Map<String, Map<String, Optional<String>>> topicConfigurationChanges;
    List<Topic> topicsToCreate;
    List<ExistingTopic> topicsToDelete;
    Map<String, Map<String, Optional<String>>> brokerConfigurationChanges;
    List<Acl> aclsToCreate;
    List<Acl> aclsToDelete;
  }

  @Value
  @With
  static class Acl {
    String resourceType;
    String name;
    String patternType;
    Collection<Permission> permissions;
  }

  @Value
  @With
  static class Permission {
    String principal;
    String host;
    String operation;
    String permissionType;
  }
}
