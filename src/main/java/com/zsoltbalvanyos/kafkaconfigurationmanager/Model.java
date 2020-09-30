package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.*;
import lombok.Value;
import lombok.With;

public class Model {

    @Value
    @With
    static public class Configuration {
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> topics;
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> configSets;
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Acl> acls;
    }

    @Value
    @With
    static public class ExistingTopic {
        String name;
        Set<Partition> partitions;
        Map<String, String> config;
    }

    @Value
    @With
    static public class Topic {
        String name;
        Optional<Integer> partitionCount;
        Optional<Integer> replicationFactor;
        Map<String, String> config;
    }

    @Value
    @With
    static public class Partition {
        int partitionNumber;
        List<Integer> replicas;
    }

    @Value
    @With
    static public class Broker {
        int id;
        Map<String, String> config;
    }

    @Value
    @With
    static public class ExecutionPlan {
        Map<String, Map<String, String>> originalConfigs;
        Map<String, Map<Integer, Integer>> originalPartitions;
        Map<String, Collection<Partition>> replicationChanges;
        Map<String, Integer> partitionChanges;
        Map<String, Map<String, Optional<String>>> topicConfigurationChanges;
        Set<Topic> topicsToCreate;
        Set<ExistingTopic> topicsToDelete;
        Set<Acl> aclsToCreate;
        Set<Acl> aclsToDelete;
    }

    @Value
    @With
    static class Acl {
        String resourceType;
        String name;
        String patternType;
        Set<Permission> permissions;
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
