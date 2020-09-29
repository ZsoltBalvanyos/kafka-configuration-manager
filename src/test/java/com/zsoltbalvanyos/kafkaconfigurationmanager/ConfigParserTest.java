package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

import java.io.IOException;
import java.util.*;

public class ConfigParserTest {
    ConfigParser configParser = new ConfigParser("./src/test/resources/schema-test.yml");

    @Test
    public void configFileDeserializableToConfigurationModel() throws IOException {
        Model.Configuration configuration = configParser.getConfiguration();
        Set<Model.Topic> result = configParser.getRequiredState(configuration);

        assertThat(result).containsExactlyInAnyOrderElementsOf(Set.of(
            new Model.Topic(
                "topic-1",
                Optional.of(2),
                Optional.of(1),
                Map.of(
                    "flush.messages", "1",
                    "segment.index.bytes", "20",
                    "cleanup.policy", "compact"
                )
            ),
            new Model.Topic(
                "topic-2",
                Optional.of(3),
                Optional.of(5),
                Map.of(
                    "flush.messages", "60",
                    "segment.index.bytes", "100"
                )
            ),
            new Model.Topic(
                "topic-3",
                Optional.of(2),
                Optional.of(2),
                Map.of(
                    "flush.messages", "1",
                    "segment.index.bytes", "20",
                    "cleanup.policy", "delete"
                )
            )
        ));
    }
}
