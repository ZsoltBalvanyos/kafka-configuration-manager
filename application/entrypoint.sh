#!/usr/bin/env bash

java -jar kafka-configuration-manager.jar \
            -b "$BOOTSTRAP_SERVER" \
            -p /properties/command-config.properties \
            -c /config/configuration.yml \
            "$1"
