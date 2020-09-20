package com.zsoltbalvanyos.kafkaconfigurationmanager;

import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Random;
import java.util.UUID;

import static java.nio.charset.Charset.forName;

public class TestUtil {

    private static final Logger log = LoggerFactory.getLogger("TestUtil");
    private static final long seed = new Random().nextLong();

    static {
        log.info("Randomizer created with seed {}", seed);
    }

    public static final EasyRandom randomizer = new EasyRandom(
        new EasyRandomParameters()
            .seed(seed)
            .objectPoolSize(100)
            .randomizationDepth(3)
            .charset(forName("UTF-8"))
            .stringLengthRange(5, 10)
            .collectionSizeRange(1, 10)
            .scanClasspathForConcreteTypes(true)
            .overrideDefaultInitialization(false)
            .ignoreRandomizationErrors(true)
            .randomize(int.class, () -> new Random().nextInt(100))
            .randomize(Optional.class, () -> Optional.of(UUID.randomUUID().toString()))
        );
}
