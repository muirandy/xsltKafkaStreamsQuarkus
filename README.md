# Prerequisites:
https://quarkus.io/get-started/
gu install native-image


# Xslt for Kafka Streams - Quarkus & Kafka Streams Implementation

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
docker build -f src/main/docker/Dockerfile.jvm -t aytl/xslt-kafka-streams-jvm
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