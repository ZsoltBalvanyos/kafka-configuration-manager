FROM adoptopenjdk:11-jre-hotspot

ENV BOOTSTRAP_SERVER=localhost:9093

COPY ./build/libs/kafka-configuration-manager-0.0.1.jar /kafka-configuration-manager.jar

CMD java -jar kafka-configuration-manager.jar \
        -b=${BOOTSTRAP_SERVER} \
        -c=/config/configuration.yml \
        -t=$JAVA_HOME/lib/security/cacerts
