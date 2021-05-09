package com.zsoltbalvanyos.kafkaconfigurationmanager;

import static com.zsoltbalvanyos.kafkaconfigurationmanager.Model.*;
import static picocli.CommandLine.*;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.Callable;
import lombok.SneakyThrows;
import lombok.Value;
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
  String propertiesPath;

  final Reporter reporter = new Reporter();
  final Mappers mappers = new Mappers();

  @Value
  class Context {
    Admin admin = buildAdmin();
    Queries queries = new Queries(admin);
    Commands commands = new Commands(admin, mappers);
    CurrentState currentState =
        new CurrentState(queries.getExistingTopics(), queries.getBrokers(), queries.getAcls());
  }

  public static void main(String[] args) {
    log.info("Kafka Configuration Manager started");
    int exitCode = new CommandLine(new Main()).execute(args);
    log.info("Kafka Configuration Manager exited");
    System.exit(exitCode);
  }

  @Override
  public Integer call() {
    describe();
    return CommandLine.ExitCode.OK;
  }

  @Command(name = "describe")
  public void describe() {
    Context context = new Context();
    try {
      log.info(reporter.stringify(context.currentState));
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      context.admin.close();
    }
  }

  @Command(name = "plan")
  public void plan() {
    Context context = new Context();
    try {
      DeltaCalculator deltaCalculator =
          new DeltaCalculator(context.currentState, buildRequiredState());
      log.info(reporter.stringify(buildExecutionPlan(deltaCalculator), context.currentState));
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      context.admin.close();
    }
  }

  @Command(name = "apply")
  public void apply() {
    Context context = new Context();
    try {
      DeltaCalculator deltaCalculator =
          new DeltaCalculator(context.currentState, buildRequiredState());
      ExecutionPlan executionPlan = buildExecutionPlan(deltaCalculator);
      log.info(reporter.stringify(executionPlan, context.currentState));
      new Executor(context.commands, context.queries, deltaCalculator)
          .run(executionPlan, context.currentState);
    } catch (Exception e) {
      log.error(e.getMessage());
      throw e;
    } finally {
      context.admin.close();
    }
  }

  private RequiredState buildRequiredState() {
    final ConfigParser configParser = new ConfigParser(configurationsPath);
    final Configuration configuration = configParser.getConfiguration();
    return new RequiredState(
        configParser.getRequiredState(configuration),
        configuration.getBrokerConfig(),
        configuration.getAcls());
  }

  private ExecutionPlan buildExecutionPlan(DeltaCalculator deltaCalculator) {
    return new ExecutionPlan(
        deltaCalculator.replicationUpdate(),
        deltaCalculator.partitionUpdate(),
        deltaCalculator.topicConfigUpdate(),
        deltaCalculator.topicsToCreate(),
        deltaCalculator.topicsToDelete(),
        deltaCalculator.brokerConfigUpdate(),
        deltaCalculator.aclsToCreate(),
        deltaCalculator.aclsToDelete());
  }

  @SneakyThrows
  private Admin buildAdmin() {
    Properties properties = new Properties();
    if (Objects.nonNull(propertiesPath) && Files.exists(Path.of(propertiesPath))) {
      properties.load(new FileReader(propertiesPath));
    }
    properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
    return Admin.create(properties);
  }
}
