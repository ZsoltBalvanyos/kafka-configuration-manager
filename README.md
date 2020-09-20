# Kafka Configuration Manager

The Kafka Configuration Manager is a configuration change management tool for Kafka that enables you to run admin tasks by defining them in a yaml file. 

### Supported Kafka Broker version
2.3 and above.

### Running locally
`./gradlew clean run --args="-b=localhost:9093 -c=/path/to/config.yml"`

If you want to see the changes only without applying them add the option `-p`.

To use SSL protocol provide the location to your truststore by the `-t=/path/to/truststore` option.

### Running Docker 
`docker run -e BOOTSTRAP_SERVER=kafka:9092 --mount type=bind,source=/path/to/config.yml,target=/config/configuration.yml zbalvanyos/kafka-configuration-manager:0.0.1`

### Getting Started

Beside the endpoint of the cluster the changes should apply to the only other mandatory 
parameter is the path to the configuration file. The following is an example of how this file has to be structured:

```yaml
topics:
  - name: topic-1
    partitionCount: 10
    replicationFactor: 1
    configSetName: durable
  - name: topic-2
    partitionCount: 12
    replicationFactor: 3
    configOverrides:
      flush.messages: 60
      segment.index.bytes: 100
  - name: topic-3
    partitionCount: 200
    replicationFactor: 3
    configSetName: performant
    configOverrides:
      cleanup.policy : delete
  - name: topic-4
    partitionCount: 2
    replicationFactor: 2
    
configSets:
  - name: durable
    configs:
      min.insync.replicas: 2
      flush.messages: 1
  - name: performant
    configs:
      min.insync.replicas: 1
      segment.index.bytes: 20
      retention.ms : 1000
```

You can create and name config sets that can be referenced from the topics, but these can be overridden on each topic. 

For every missing configuration the broker default is going to be used. Setting partition count and replication factor is mandatory. 

If a topic is deleted from the configuration file and the `delete.topic.enable` broker property is set to true, the topic will be deleted!
