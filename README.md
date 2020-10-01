# Kafka Configuration Manager

The Kafka Configuration Manager is a configuration change management tool for Kafka that enables you to run admin tasks by defining them in a yaml file. 

### Supported Kafka Broker version
2.3 and above.

### Running locally
`./gradlew clean run --args="-b=localhost:9093 -c=/path/to/config.yml plan"`

If you want to see the changes only without applying them add the option `-p`.

To use SSL protocol provide the location to your truststore by the `-t=/path/to/truststore` option.

### Running Docker 
`docker run -e BOOTSTRAP_SERVER=kafka:9092 --mount type=bind,source=/path/to/config.yml,target=/config/configuration.yml zbalvanyos/kafka-configuration-manager:0.0.1`

### Getting Started

##### Operations

- describe: prints the current state of the cluster
- plan: prints the difference between the current state of the cluster and the configuration file
- apply: update the cluster according to the configuration file

##### Configuration file
The following is an example of how the configuration file has to be structured:

```yaml
topics:
  - name: topic-1
    configName: default

  - name: topic-2
    partitionCount: 3
    replicationFactor: 5
    flush.messages: 60
    segment.index.bytes: 100

  - name: topic-3
    configName: default
    replicationFactor: 2
    cleanup.policy : delete

configSets:
  - name: default
    partitionCount: 2
    replicationFactor: 1
    flush.messages: 1
    segment.index.bytes: 20
    cleanup.policy : compact
```

You can create and name config sets that can be referenced from the topics, but values of these can be overridden on each topic. 

For every missing configuration the broker default is going to be used. 
