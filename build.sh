#!/usr/bin/env bash


rm -rf ksuite/
./gradlew clean distZip
unzip build/distributions/ksuite.zip