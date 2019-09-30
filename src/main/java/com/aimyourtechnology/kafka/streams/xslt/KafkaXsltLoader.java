package com.aimyourtechnology.kafka.streams.xslt;

import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

class KafkaXsltLoader implements XsltLoader {
    private final String bootstrapServers;
    private final String xsltKafkaTopic;

    public KafkaXsltLoader(String bootstrapServers, String xsltKafkaTopic) {
        this.bootstrapServers = bootstrapServers;
        this.xsltKafkaTopic = xsltKafkaTopic;
    }

    @Override
    public String loadXslt(String xsltName) {
        Properties props = createXsltConsumerProperties();
        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(xsltKafkaTopic));

            int loopCount = 10;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    String key = record.key();
                    String value = record.value();
                    System.out.printf("key = %s, value = %s%n", key, value);
                    if (key != null && value != null)
                        if (key.equals(xsltName)) {
                            return value;
                        }

                    loopCount++;
                }
                if (loopCount >= 10) {
                    loopCount = 0;
                    System.out.printf("Looking on topic %s for xslt %s", xsltKafkaTopic, xsltName);
                }
            }

        }
    }

    private Properties createXsltConsumerProperties() {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, createUniqueGroupId());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    private String createUniqueGroupId() {
        return UUID.randomUUID().toString();
    }
}
