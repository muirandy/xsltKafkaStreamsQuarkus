package com.aimyourtechnology.kafka.streams.xslt.acceptance;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsOptions;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.xmlunit.assertj.XmlAssert;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
public class XsltTransformShould {
    @Container
    protected static final KafkaContainer KAFKA_CONTAINER = new KafkaContainer("5.3.0").withEmbeddedZookeeper()
                                                                                       .waitingFor(Wait.forLogMessage(".*Launching kafka.*\\n", 1))
                                                                                       .waitingFor(Wait.forLogMessage(".*started.*\\n", 1));
    private static final String INPUT_TOPIC = "xmlInput";
    private static final String OUTPUT_TOPIC = "xmlOutput";
    private static final String XSLT_TOPIC = "xslt";
    private static final String ENV_KEY_KAFKA_BROKER_SERVER = "KAFKA_BROKER_SERVER";
    private static final String ENV_KEY_KAFKA_BROKER_PORT = "KAFKA_BROKER_PORT";
    private static final String XSLT_NAME = "template.xslt";
    private static final String KAFKA_STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    private static final String KAFKA_STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    @Container
    private GenericContainer converterContainer = new GenericContainer("aytl/xslt-kafka-streams:latest")
            .withNetwork(KAFKA_CONTAINER.getNetwork())
            .withEnv(calculateEnvProperties())
            .waitingFor(Wait.forLogMessage(".*Stream manager initializing.*\\n", 1))
            .waitingFor(Wait.forLogMessage(".*Subscribed to topic.*xslt.*\\n", 1));
    private String xmlKey = UUID.randomUUID().toString();
    private String randomValue = UUID.randomUUID().toString();

    private Map<String, String> calculateEnvProperties() {
        createTopics();
        Map<String, String> envProperties = new HashMap<>();
        String bootstrapServers = KAFKA_CONTAINER.getNetworkAliases().get(0);
        envProperties.put(ENV_KEY_KAFKA_BROKER_SERVER, bootstrapServers);
        envProperties.put(ENV_KEY_KAFKA_BROKER_PORT, "" + 9092);
        envProperties.putAll(createCustomEnvProperties());
        return envProperties;
    }

    private void createTopics() {
        AdminClient adminClient = AdminClient.create(getKafkaProperties());

        CreateTopicsResult createTopicsResult = adminClient.createTopics(getTopics(), new CreateTopicsOptions().timeoutMs(1000));
        Map<String, KafkaFuture<Void>> futureResults = createTopicsResult.values();
        futureResults.values().forEach(f -> {
            try {
                f.get(1000, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            } catch (TimeoutException e) {
                e.printStackTrace();
            }
        });
        adminClient.close();
    }

    protected static Properties getKafkaProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_STRING_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_STRING_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, XsltTransformShould.class.getName());
        return props;
    }

    protected List<NewTopic> getTopics() {
        return getTopicNames().stream()
                              .map(n -> new NewTopic(n, 1, (short) 1))
                              .collect(Collectors.toList());
    }

    protected List<String> getTopicNames() {
        List<String> topicNames = new ArrayList<>();
        topicNames.add(INPUT_TOPIC);
        topicNames.add(OUTPUT_TOPIC);
        topicNames.add(XSLT_TOPIC);
        return topicNames;
    }

    protected Map<String, String> createCustomEnvProperties() {
        Map<String, String> envProperties = new HashMap<>();
        envProperties.put("INPUT_KAFKA_TOPIC", INPUT_TOPIC);
        envProperties.put("OUTPUT_KAFKA_TOPIC", OUTPUT_TOPIC);
        envProperties.put("XSLT_KAFKA_TOPIC", XSLT_TOPIC);
        envProperties.put("XSLT_NAME", XSLT_NAME);
        envProperties.put("APP_NAME", "xsltKafkaStreams");
        return envProperties;
    }

    @Test
    void transformXmlUsingXslt() {
        writeXsltToXsltTopic();

        writeXmlToInputTopic();

        assertTransformedXmlOnOutputTopic();
    }

    private void writeXsltToXsltTopic() {
        try {
            getStringStringKafkaProducer().send(createProducerRecord(XSLT_TOPIC, XSLT_NAME, createXsltMessage())).get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to write Xslt to XSLT Topic:", e);
            e.printStackTrace();
        }
    }

    private KafkaProducer<String, String> getStringStringKafkaProducer() {
        return new KafkaProducer<>(kafkaPropertiesForProducer());
    }

    private static Properties kafkaPropertiesForProducer() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_CONTAINER.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KAFKA_STRING_SERIALIZER);
        return props;
    }

    private <K, V> ProducerRecord createProducerRecord(String topicName, K key, V message) {
        return new ProducerRecord<K, V>(topicName, key, message);
    }

    private String createXsltMessage() {
        return "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n" +
                "    <xsl:template match=\"/\">\n" +
                "        <order>\n" +
                "            <xsl:attribute name=\"orderId\">\n" +
                "                <xsl:value-of select=\"order/orderId\"/>\n" +
                "            </xsl:attribute>\n" +
                "        </order>\n" +
                "    </xsl:template>\n" +
                "</xsl:stylesheet>";
    }

    private void writeXmlToInputTopic() {
        try {
            getStringStringKafkaProducer().send(createProducerRecord(INPUT_TOPIC, xmlKey, createXmlMessage())).get();
        } catch (InterruptedException | ExecutionException e) {
            fail("Unable to write Xml to Input Topic:", e);
            e.printStackTrace();
        }
    }

    private String createXmlMessage() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order>" +
                        "<orderId>%s</orderId>\n" +
                        "</order>", randomValue
        );
    }

    private void assertTransformedXmlOnOutputTopic() {
        Optional<ConsumerRecord<String, String>> resultFromTopic = readFromOutputTopic();
        assertTrue(resultFromTopic.isPresent());
        ConsumerRecord<String, String> consumerRecord = resultFromTopic.get();
        String resultingMessagePayload = consumerRecord.value();
        XmlAssert.assertThat(resultingMessagePayload).and(createTransformedXmlMessage())
                 .ignoreWhitespace()
                 .areIdentical();
    }

    private Optional<ConsumerRecord<String, String>> readFromOutputTopic() {
        ConsumerRecords<String, String> consumerRecords = pollForResults();
        assertFalse(consumerRecords.isEmpty());
        assertEquals(1, consumerRecords.count());

        Spliterator<ConsumerRecord<String, String>> spliterator = Spliterators.spliteratorUnknownSize(consumerRecords.iterator(), 0);
        Stream<ConsumerRecord<String, String>> consumerRecordStream = StreamSupport.stream(spliterator, false);
        return consumerRecordStream.findAny();
    }

    private ConsumerRecords<String, String> pollForResults() {
        KafkaConsumer<String, String> consumer = createKafkaConsumer();
        consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));
        Duration duration = Duration.ofSeconds(4);
        return consumer.poll(duration);
    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = getConsumerProperties();
        return new KafkaConsumer<>(props);
    }

    private Properties getConsumerProperties() {
        String bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KAFKA_STRING_DESERIALIZER);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KAFKA_STRING_DESERIALIZER);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "100");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.getClass().getName());
        return props;
    }

    private String createTransformedXmlMessage() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order id=\"%s\"/>",
                randomValue
        );
    }

}
