package com.zsoltbalvanyos.kafkaconfigurationmanager;

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

    public Model.Configuration getConfiguration() throws IOException {
        return new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .readValue(new File(pathToConfigs), Model.Configuration.class);
    }

    public Set<Model.Topic> getRequiredState(Model.Configuration configuration) {

        Map<String, Map<String, String>> configSetMap =
            configuration
                .getConfigSets()
                .stream()
                .collect(Collectors.toMap(Model.ConfigSet::getName, Model.ConfigSet::getConfigs));

        return configuration
            .getTopics()
            .stream()
            .map(topic -> {
                Map<String, String> normalizedConfigs = topic
                    .getConfigSetName()
                    .map(configSetName -> {
                        if (!configSetMap.containsKey(configSetName)) {
                            throw new RuntimeException("No config set has been defined with name " + configSetName);
                        }
                        Map<String, String> configSetOfThisTopic = new HashMap<>(configSetMap.get(configSetName));
                        configSetOfThisTopic.putAll(topic.getConfigOverrides());
                        return configSetOfThisTopic; })
                    .orElse(topic.getConfigOverrides());

                return new Model.Topic(
                    topic.getName(),
                    topic.getPartitionCount(),
                    topic.getReplicationFactor(),
                    normalizedConfigs); })
            .collect(Collectors.toSet());
    }

}
