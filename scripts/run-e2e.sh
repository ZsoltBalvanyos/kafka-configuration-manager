#!/usr/bin/env bash

cd application || exit
gradle clean build
docker build -t kafka-configuration-manager .

cd ../e2e || exit
gradle clean build

