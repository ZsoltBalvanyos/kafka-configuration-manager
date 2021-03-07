package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ConfigParser {

  final String pathToConfigs;

  public Configuration getConfiguration() throws IOException {
    return new ObjectMapper(new YAMLFactory())
        .registerModule(new Jdk8Module())
        .readValue(new File(pathToConfigs), Configuration.class);
  }

  public Set<Topic> getRequiredState(Configuration configuration) {

    Map<String, Map<String, String>> configSetMap = new HashMap<>();

    configuration
        .getConfigSets()
        .forEach(
            configSet -> {
              if (!configSet.containsKey("name") || configSet.get("name").isBlank()) {
                throw new RuntimeException("Configuration set must have a non-blank name.");
              }
              String name = configSet.get("name");
              configSet.remove("name");
              configSetMap.put(name, configSet);
            });

    return configuration.getTopics().stream()
        .map(t -> buildTopic(t, configSetMap))
        .collect(Collectors.toSet());
  }

  protected Topic buildTopic(
      Map<String, String> topicDescription, Map<String, Map<String, String>> configSetMap) {
    if (!topicDescription.containsKey("name") || topicDescription.get("name").isEmpty()) {
      throw new RuntimeException("Topic definition must have a non-empty name");
    }
    String name = topicDescription.get("name");
    topicDescription.remove("name");

    Map<String, String> topicConfigs =
        Optional.ofNullable(topicDescription.get("configName"))
            .map(
                configName -> new HashMap<>(configSetMap.getOrDefault(configName, new HashMap<>())))
            .orElse(new HashMap<>());

    Optional<Integer> partitionCount =
        Optional.ofNullable(topicDescription.get("partitionCount"))
            .or(() -> Optional.ofNullable(topicConfigs.get("partitionCount")))
            .map(Integer::valueOf);

    Optional<Integer> replicationFactor =
        Optional.ofNullable(topicDescription.get("replicationFactor"))
            .or(() -> Optional.ofNullable(topicConfigs.get("replicationFactor")))
            .map(Integer::valueOf);

    if (replicationFactor.isPresent()
        && replicationFactor.get() > Short.toUnsignedInt(Short.MAX_VALUE)) {
      throw new RuntimeException(
          String.format(
              "Replication factor %d of topic %s is greater than %d",
              replicationFactor.get(), name, Short.MAX_VALUE));
    }

    topicConfigs.putAll(topicDescription);

    topicConfigs.remove("partitionCount");
    topicConfigs.remove("replicationFactor");
    topicConfigs.remove("configName");

    return new Topic(name, partitionCount, replicationFactor, topicConfigs);
  }
}
