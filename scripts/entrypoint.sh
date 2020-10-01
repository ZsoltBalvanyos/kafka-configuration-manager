#!/usr/bin/env bash

java -jar kafka-configuration-manager.jar \
            -b "$BOOTSTRAP_SERVER" \
            -c /config/configuration.yml \
            -t "$JAVA_HOME"/lib/security/cacerts \
            "$1"
