package com.zsoltbalvanyos.kafkaconfigurationmanager;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class Main implements Callable<Integer> {

    // TODO: 13/09/2020 validation implementation
    // TODO: 13/09/2020 acl management
    // TODO: 13/09/2020 rollback

    Logger log = LoggerFactory.getLogger(getClass().getName());

    @Option(names = { "-b", "--bootstrap-server"})
    String bootstrapServer;

    @Option(names = { "-c", "--configurations"})
    String configurationsPath;

    @Option(names = { "-p", "--plan"})
    boolean plan;

    @Option(names = { "-t", "--truststore-location"})
    String truststoreLocation;

    public static void main(String[] args) {
        System.exit(new CommandLine(new Main()).execute(args));
    }

    @Override
    public Integer call() throws Exception {

        String action = plan ? "plan" : "update";
        log.info("Starting to {} configuration of cluster {}", action, bootstrapServer);

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        if (truststoreLocation != null) {
            properties.put(AdminClientConfig.SECURITY_PROTOCOL_CONFIG, SecurityProtocol.SSL.name);
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststoreLocation);
        }

        Admin adminClient = Admin.create(properties);

        KafkaClient kafkaClient = new KafkaClient(adminClient);
        ConfigParser configParser = new ConfigParser(configurationsPath);
        DeltaCalculator deltaCalculator = new DeltaCalculator(kafkaClient.getAllBrokers());
        Reporter reporter = new Reporter();

        Set<Model.ExistingTopic> currentState = kafkaClient.getExistingTopics();
        Set<Model.Topic> requiredState = configParser.getRequiredState(configParser.getConfiguration());

        try {
            process(kafkaClient, deltaCalculator, reporter, currentState, requiredState);
        } catch (Exception e) {
            e.printStackTrace();
            log.error(e.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        return CommandLine.ExitCode.OK;
    }

    public void process(KafkaClient kafkaClient,
                        DeltaCalculator deltaCalculator,
                        Reporter reporter,
                        Set<Model.ExistingTopic> currentState,
                        Set<Model.Topic> requiredState) throws ExecutionException, InterruptedException {

        log.info("Current state: {}", currentState);
        log.info("Required state: {}", requiredState);

        Map<String, Collection<Model.Partition>> replicationChanges = deltaCalculator.replicationUpdate(currentState, requiredState);
        Map<String, Integer> partitionChanges = deltaCalculator.partitionUpdate(currentState, requiredState);
        Map<String, Map<String, Optional<String>>> topicConfigurationChanges = deltaCalculator.topicConfigUpdate(currentState, requiredState);
        Set<Model.Topic> topicsToCreate = deltaCalculator.topicsToCreate(currentState, requiredState);
        Set<Model.ExistingTopic> topicsToDelete = deltaCalculator.topicsToDelete(currentState, requiredState);

        reporter.printChanges(
            replicationChanges,
            partitionChanges,
            topicConfigurationChanges,
            topicsToCreate,
            topicsToDelete
        );

        if (!plan) {

            if (!replicationChanges.isEmpty() && kafkaClient.noOngoingReassignment()) {
                kafkaClient.updateReplication(replicationChanges);
            }

            if (!partitionChanges.isEmpty() && kafkaClient.noOngoingReassignment()) {
                kafkaClient.updatePartitions(partitionChanges);
            }

            if (!topicConfigurationChanges.isEmpty()) {
                kafkaClient.updateConfigOfTopics(topicConfigurationChanges);
            }

            if (!topicsToCreate.isEmpty()) {
                kafkaClient.createTopics(topicsToCreate);
            }

            if (!topicsToDelete.isEmpty()) {
                kafkaClient.deleteTopics(topicsToDelete);
            }
        }
    }
}
