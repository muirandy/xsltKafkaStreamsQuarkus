package com.aimyourtechnology.kafka.streams.xslt;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueMapper;

import java.util.Properties;
import java.util.function.Function;

public class TopologyProducer {
    private String bootstrapServers;
    private String appName;
    private String xslt;
    private String inputTopic;
    private String outputTopic;
    private KafkaStreams streams;

    public TopologyProducer(String bootstrapServers, String appName, String inputTopic, String outputTopic) {
        this.bootstrapServers = bootstrapServers;
        this.appName = appName;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    void runTopology() {
        Topology topology = buildTopology();
        //        streams = kafkaStreamsTracing.kafkaStreams(topology, streamingConfig);
        streams = new KafkaStreams(topology, createStreamingConfig());
        streams.start();
    }

    private Topology buildTopology() {
        StreamsBuilder builder = new StreamsBuilder();

        Function<String, String> transformer = createTransformer();
        ValueMapper<String, String> stringMapper = xmlString -> transformer.apply(xmlString);

        KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> stringStream = inputStream.mapValues(stringMapper);
        stringStream.to(outputTopic);

        return builder.build();
    }

    private Function<String, String> createTransformer() {
        return XsltTransformer::transform;
    }

    private Properties createStreamingConfig() {
        Properties streamingConfig;
        streamingConfig = new Properties();
        streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        streamingConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamingConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamingConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        return streamingConfig;
    }

    void shutdown() {
        streams.close();
    }

}
