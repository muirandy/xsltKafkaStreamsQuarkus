#!/usr/bin/env bash

export KAFKA_BROKER_SERVER="localhost"
export KAFKA_BROKER_PORT="9092"
export XSLT_NAME="knitwareModifyVoip.xslt"
export XSLT_KAFKA_TOPIC="XSLT"
export INPUT_KAFKA_TOPIC="XML_SINK_MODIFY_VOIP_INSTRUCTIONS_WITH_SWITCH_ID"
export OUTPUT_KAFKA_TOPIC="switch.modification.instructions"
export APP_NAME="sns-incoming-operator-messages-converter4"

java -Xmx48M -jar ./target/xsltKafkaStreamsQuarkus-1.0-SNAPSHOT-runner.jar