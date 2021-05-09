package com.zsoltbalvanyos.kafkaconfigurationmanager;

import io.github.xshadov.easyrandom.vavr.VavrRandomizerRegistry;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.Random;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.resource.PatternType;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.jeasy.random.FieldPredicates;
import org.jeasy.random.randomizers.misc.EnumRandomizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUtil {

  private static final Logger log = LoggerFactory.getLogger("TestUtil");
  private static final long seed = new Random().nextLong();

  static {
    log.info("Randomizer created with seed {}", seed);
  }

  public static final VavrRandomizerRegistry vavrRandomizerRegistry = new VavrRandomizerRegistry();

  public static EasyRandom randomizer() {

    EasyRandom random =
        new EasyRandom(
            new EasyRandomParameters()
                .randomizerRegistry(vavrRandomizerRegistry)
                .seed(seed)
                .objectPoolSize(100)
                .randomizationDepth(3)
                .charset(StandardCharsets.UTF_8)
                .stringLengthRange(5, 10)
                .collectionSizeRange(1, 10)
                .scanClasspathForConcreteTypes(true)
                .overrideDefaultInitialization(false)
                .ignoreRandomizationErrors(true)
                .randomize(int.class, () -> new Random().nextInt(100))
                .randomize(Optional.class, () -> Optional.of(new Random().nextInt(100)))
                .randomize(
                    FieldPredicates.named("patternType")
                        .and(FieldPredicates.inClass(Model.Acl.class)),
                    () ->
                        new EnumRandomizer<>(PatternType.class, PatternType.ANY, PatternType.MATCH)
                            .getRandomValue()
                            .name())
                .randomize(
                    FieldPredicates.named("operation")
                        .and(FieldPredicates.inClass(Model.Permission.class)),
                    () ->
                        new EnumRandomizer<>(AclOperation.class, AclOperation.ANY)
                            .getRandomValue()
                            .name())
                .randomize(
                    FieldPredicates.named("permissionType")
                        .and(FieldPredicates.inClass(Model.Permission.class)),
                    () ->
                        new EnumRandomizer<>(AclPermissionType.class, AclPermissionType.ANY)
                            .getRandomValue()
                            .name()));
    vavrRandomizerRegistry.setEasyRandom(random);
    return random;
  }
}
