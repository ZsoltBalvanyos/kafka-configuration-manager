package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static java.util.stream.Collectors.toMap;
import static picocli.CommandLine.*;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;

@Slf4j
public class Main implements Callable<Integer> {

  @Option(names = {"-b", "--bootstrap-server"})
  String bootstrapServer;

  @Option(names = {"-c", "--configurations"})
  String configurationsPath;

  @Option(names = {"-p", "--properties"})
  String propertiesLocation;

  Reporter reporter = new Reporter();

  public static void main(String[] args) {
    log.info("Kafka Configuration Manager started");
    int exitCode = new CommandLine(new Main()).execute(args);
    log.info("Kafka Configuration Manager exited");
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws Exception {
    describe();
    return CommandLine.ExitCode.OK;
  }

  @Command(name = "describe")
  public void describe() throws IOException {
    KafkaClient kafkaClient = getKafkaClient();
    try {
      log.info(
          reporter.print(
              kafkaClient.getExistingTopics(), kafkaClient.getAllBrokers(), kafkaClient.getAcls()));
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      kafkaClient.close();
    }
  }

  @Command(name = "plan")
  public void plan() throws IOException {
    KafkaClient kafkaClient = getKafkaClient();
    try {
      DeltaCalculator deltaCalculator = new DeltaCalculator(kafkaClient.getAllBrokers());
      log.info(reporter.print(getExecutionPlan(kafkaClient, deltaCalculator)));
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      kafkaClient.close();
    }
  }

  @Command(name = "apply")
  public void apply() throws Exception {
    KafkaClient kafkaClient = getKafkaClient();
    try {
      Set<Broker> allBrokers = kafkaClient.getAllBrokers();
      DeltaCalculator deltaCalculator = new DeltaCalculator(allBrokers);
      ExecutionPlan executionPlan = getExecutionPlan(kafkaClient, deltaCalculator);
      log.info(reporter.print(executionPlan));
      new Executor(kafkaClient, deltaCalculator).run(executionPlan, allBrokers);
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      kafkaClient.close();
    }
  }

  private ExecutionPlan getExecutionPlan(KafkaClient kafkaClient, DeltaCalculator deltaCalculator)
      throws IOException {
    ConfigParser configParser = new ConfigParser(configurationsPath);
    Configuration configuration = configParser.getConfiguration();
    Collection<Topic> requiredState = configParser.getRequiredState(configuration);

    Collection<ExistingTopic> existingTopics = kafkaClient.getExistingTopics();

    Map<String, Map<Integer, Integer>> originalPartitions =
        existingTopics.stream()
            .collect(
                toMap(
                    ExistingTopic::getName,
                    t ->
                        t.getPartitions().stream()
                            .collect(
                                toMap(
                                    Partition::getPartitionNumber, p -> p.getReplicas().size()))));

    Map<String, Map<String, String>> originalConfigs =
        existingTopics.stream().collect(toMap(ExistingTopic::getName, ExistingTopic::getConfig));

    Set<Acl> requiredAcls = configuration.getAcls();
    Set<Acl> currentAcls = kafkaClient.getAcls();

    return new ExecutionPlan(
        originalConfigs,
        originalPartitions,
        deltaCalculator.replicationUpdate(existingTopics, requiredState),
        deltaCalculator.partitionUpdate(existingTopics, requiredState),
        deltaCalculator.topicConfigUpdate(existingTopics, requiredState),
        deltaCalculator.topicsToCreate(existingTopics, requiredState),
        deltaCalculator.topicsToDelete(existingTopics, requiredState),
        deltaCalculator.brokerConfigUpdate(configuration.getBrokerConfig()),
        deltaCalculator.aclsToCreate(currentAcls, requiredAcls),
        deltaCalculator.aclsToDelete(currentAcls, requiredAcls));
  }

  private KafkaClient getKafkaClient() throws IOException {
    Properties properties = new Properties();
    if (Objects.nonNull(propertiesLocation) && Files.exists(Path.of(propertiesLocation))) {
      properties.load(new FileReader(propertiesLocation));
    }
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

    Admin admin = Admin.create(properties);
    return new KafkaClient(admin, 15);
  }
}
