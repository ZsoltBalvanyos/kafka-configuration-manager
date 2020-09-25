package com.zsoltbalvanyos.kafkaconfigurationmanager;

import com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import lombok.AllArgsConstructor;

import java.util.*;
import java.io.File;
import java.io.IOException;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ConfigParser {

    final String pathToConfigs;

    public Configuration getConfiguration() throws IOException {
        return new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .readValue(new File(pathToConfigs), Configuration.class);
    }

    public Set<Model.Topic> getRequiredState(Configuration configuration) {

        Map<String, Map<String, String>> configSetMap = new HashMap<>();

        configuration
            .getConfigSets()
            .forEach(configSet -> {
                if (!configSet.containsKey("name") || configSet.get("name").isEmpty()) {
                    throw new RuntimeException("Configuration set must have a non-empty name.");
                }
                String name = configSet.get("name");
                configSet.remove("name");
                configSetMap.put(name, configSet);
            });

        return configuration
            .getTopics()
            .stream()
            .map(t -> buildTopic(t, configSetMap))
            .collect(Collectors.toSet());
    }

    protected Topic buildTopic(Map<String, String> topicDescription, Map<String, Map<String, String>> configSetMap) {
        if (!topicDescription.containsKey("name") || topicDescription.get("name").isEmpty()) {
            throw new RuntimeException("Topic definition must have a non-empty name");
        }
        String name = topicDescription.get("name");
        topicDescription.remove("name");

        Map<String, String> topicConfigs = new HashMap<>();

        Optional
            .ofNullable(topicDescription.get("configName"))
            .ifPresent(configName -> topicConfigs.putAll(configSetMap.getOrDefault(configName, new HashMap<>())));

        Optional<Integer> partitionCount = Optional.ofNullable(topicDescription.get("partitionCount")).map(Integer::valueOf);
        Optional<Integer> replicationFactor = Optional.ofNullable(topicDescription.get("replicationFactor")).map(Integer::valueOf);
        topicDescription.remove("partitionCount");
        topicDescription.remove("replicationFactor");
        topicDescription.remove("configName");

        topicConfigs.putAll(topicDescription);

        return new Topic(
            name,
            partitionCount,
            replicationFactor,
            topicConfigs);
    }

}
