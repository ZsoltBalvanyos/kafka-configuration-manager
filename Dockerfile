FROM adoptopenjdk:11-jre-hotspot

ENV BOOTSTRAP_SERVER=localhost:9093

COPY ./build/libs/kafka-configuration-manager-0.0.1.jar /kafka-configuration-manager.jar
COPY ./scripts/entrypoint.sh /entrypoint.sh

RUN chmod u+x /entrypoint.sh
RUN cp $JAVA_HOME/lib/security/cacerts /kafka.client.truststore.jks

ENTRYPOINT ["/entrypoint.sh"]
CMD ["describe"]
