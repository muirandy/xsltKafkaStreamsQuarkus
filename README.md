# Prerequisites:
https://quarkus.io/get-started/
gu install native-image

# Xslt for Kafka Streams - Quarkus & Kafka Streams Implementation

# Run
./mvnw compile quarkus:dev -Ddebug -DENV_KEY_KAFKA_BROKER_SERVER=broker -DENV_KEY_KAFKA_BROKER_PORT=9092 -DINPUT_KAFKA_TOPIC=XML_SINK_MODIFY_VOIP_INSTRUCTIONS_WITH_SWITCH_ID -DOUTPUT_KAFKA_TOPIC=switch.modification.instructions -DXSLT_KAFKA_TOPIC=XSLT -DXSLT_NAME=cujo.xslt -DAPP_NAME=cusjoXslt

## JVM (least beneficial!)
### Build
```
mvn package 
```

### Run
```
./macJdk.sh
```
## JVM Image on Docker
### Build
```
mvn package
docker build -f src/main/docker/Dockerfile.jvm -t aytl/xslt-kafka-streams-jvm .
```

## Native Image on macOS
### Build
```
mvn package -Pnative
```

### Run
```
./macNative.sh
```

## Native Image on Docker
### Build
```
mvn package -Pnative -Dnative-image.docker-build=true
docker build -f src/main/docker/Dockerfile.native -t aytl/xslt-kafka-streams .
```

### Run
```
./dockerNative.sh
```

## Acceptance Tests
Run any of the tests from `test/java/com/aimyourtechnology/kafka/streams/xslt/acceptance`. You will need a Docker environment, but the tests will spin 
up Kafka (so make sure its not already running!).

