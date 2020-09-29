package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.*;
import lombok.Value;
import lombok.With;

public class Model {

    @Value
    @With
    static class Configuration {
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> topics;
        @JsonSetter(nulls = Nulls.AS_EMPTY) Set<Map<String, String>> configSets;
    }

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
        List<Integer> replicas;
    }

    @Value
    @With
    static class Broker {
        int id;
        Map<String, String> config;
    }
}
