#!/usr/bin/env bash

gradle :application:clean :application:build
gradle :e2e:clean :e2e:compileJava

docker build -t kafka-configuration-manager application

docker-compose -f e2e/src/test/resources/acl-compose.yml up -d
OUTPUT=$(docker logs kafka_1)

while [[ "$OUTPUT" != *"[KafkaServer id=1] started"* ]]
do
  OUTPUT=$(docker logs kafka_1)
done

gradle :e2e:test --tests ACLTest
RESULT=$?

docker-compose -f e2e/src/test/resources/acl-compose.yml down

exit $RESULT
