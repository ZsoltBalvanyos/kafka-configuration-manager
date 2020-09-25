package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.*;
import lombok.Value;
import lombok.With;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

public class Model {

    @Value
    @With
    static class Configuration {
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> topics;
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> configSets;
    }

//    @Value
//    @With
//    static class TopicDescription {
//        @NotBlank(message = "Topic name is mandatory") String name;
//        Optional<@NotBlank String> configSetName;
//        @Min(value = 1, message = "PartitionCount must be a positive integer") int partitionCount;
//        @Min(value = 1, message = "ReplicationFactor must be a positive integer") int replicationFactor;
//        @JsonSetter(nulls = Nulls.AS_EMPTY) Map<String, String> configOverrides;
//    }
//
//    @Value
//    @With
//    static class ConfigSet {
//        @NotBlank(message = "ConfigSet name is mandatory") String name;
//        @JsonSetter(nulls = Nulls.AS_EMPTY) Map<String, String> configs;
//    }

    @Value
    @With
    static class ExistingTopic {
        String name;
        Set<Partition> partitions;
        Map<String, String> config;
    }

    @Value
    @With
    static class Topic {
        String name;
        Optional<Integer> partitionCount;
        Optional<Integer> replicationFactor;
        Map<String, String> config;
    }

    @Value
    @With
    static class Partition {
        int partitionNumber;
        List<Broker> replicas;
    }

    @Value
    @With
    static class Broker {
        int id;
    }
}
