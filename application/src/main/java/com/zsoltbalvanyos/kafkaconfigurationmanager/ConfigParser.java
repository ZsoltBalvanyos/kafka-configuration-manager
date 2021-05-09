package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import io.vavr.collection.HashMap;
import io.vavr.collection.HashSet;
import io.vavr.collection.Map;
import io.vavr.collection.Traversable;
import io.vavr.jackson.datatype.VavrModule;
import java.io.File;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;

@AllArgsConstructor
public class ConfigParser {

  final String pathToConfigs;

  @SneakyThrows
  public Configuration getConfiguration() {
    Configuration raw =
        new ObjectMapper(new YAMLFactory())
            .registerModule(new Jdk8Module())
            .registerModule(new VavrModule())
            .readValue(new File(pathToConfigs), Configuration.class);

    return new Configuration(
        Optional.ofNullable(raw.getBrokerConfig()).orElse(HashMap.empty()),
        Optional.ofNullable(raw.getTopics()).orElse(HashSet.empty()),
        Optional.ofNullable(raw.getConfigSets()).orElse(HashSet.empty()),
        Optional.ofNullable(raw.getAcls()).orElse(HashSet.empty()));
  }

  public Traversable<RequiredTopic> getRequiredState(Configuration configuration) {

    Map<String, Map<String, String>> configSetMap =
        configuration
            .getConfigSets()
            .toMap(
                configSet -> {
                  if (!configSet.containsKey("name") || configSet.get("name").get().isBlank()) {
                    throw new RuntimeException("Configuration set must have a non-blank name.");
                  }
                  String name = configSet.get("name").get();
                  configSet.remove("name");
                  return Map.entry(name, configSet);
                });

    return configuration.getTopics().map(t -> buildTopic(t, configSetMap));
  }

  protected RequiredTopic buildTopic(
      Map<String, String> topicDescription, Map<String, Map<String, String>> configSetMap) {

    if (!topicDescription.containsKey("name") || topicDescription.get("name").isEmpty()) {
      throw new RuntimeException("Topic definition must have a non-empty name");
    }

    String name = topicDescription.get("name").get();
    topicDescription.remove("name");

    Map<String, String> topicConfigs =
        topicDescription
            .get("configName")
            .map(configName -> configSetMap.get(configName).getOrElse(HashMap.empty()))
            .getOrElse(HashMap.empty());

    Optional<Integer> partitionCount =
        topicDescription
            .get("partitionCount")
            .orElse(() -> topicConfigs.get("partitionCount"))
            .map(Integer::valueOf)
            .toJavaOptional();

    Optional<Integer> replicationFactor =
        topicDescription
            .get("replicationFactor")
            .orElse(() -> topicConfigs.get("replicationFactor"))
            .map(Integer::valueOf)
            .toJavaOptional();

    if (replicationFactor.isPresent()
        && replicationFactor.get() > Short.toUnsignedInt(Short.MAX_VALUE)) {
      throw new RuntimeException(
          String.format(
              "Replication factor %d of topic %s is greater than %d",
              replicationFactor.get(), name, Short.MAX_VALUE));
    }

    topicConfigs.merge(topicDescription);

    var finalConfigs = new java.util.HashMap<String, String>(topicConfigs.toJavaMap());
    finalConfigs.putAll(topicDescription.toJavaMap());

    finalConfigs.remove("partitionCount");
    finalConfigs.remove("replicationFactor");
    finalConfigs.remove("configName");
    finalConfigs.remove("name");

    return new RequiredTopic(
        TopicName.of(name), partitionCount, replicationFactor, HashMap.ofAll(finalConfigs));
  }
}
