FROM adoptopenjdk:11-jre-hotspot

ENV BOOTSTRAP_SERVER=localhost:9092

COPY ./build/libs/application-0.0.1.jar /kafka-configuration-manager.jar
COPY ./entrypoint.sh /entrypoint.sh

RUN chmod u+x /entrypoint.sh
RUN cp $JAVA_HOME/lib/security/cacerts /kafka.client.truststore.jks

ENTRYPOINT ["/entrypoint.sh"]
CMD ["describe"]
