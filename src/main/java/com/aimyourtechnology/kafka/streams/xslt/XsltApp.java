package com.aimyourtechnology.kafka.streams.xslt;

import brave.Tracing;
import brave.kafka.streams.KafkaStreamsTracing;
import brave.sampler.Sampler;
import io.quarkus.runtime.StartupEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import zipkin2.Span;
import zipkin2.reporter.AsyncReporter;
import zipkin2.reporter.urlconnection.URLConnectionSender;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.Properties;
import java.util.logging.Logger;

import static java.util.logging.Level.INFO;

@ApplicationScoped
public class XsltApp {
    private static final Logger LOGGER = Logger.getLogger(XsltApp.class.getSimpleName());

    @ConfigProperty(name = "inputKafkaTopic")
    String inputKafkaTopic;

    @ConfigProperty(name = "outputKafkaTopic")
    String outputKafkaTopic;

    @ConfigProperty(name = "xsltKafkaTopic")
    String xsltKafkaTopic;

    @ConfigProperty(name = "xsltName")
    String xsltName;

    @ConfigProperty(name = "appName")
    String appName;

    @ConfigProperty(name = "kafkaBrokerServer")
    String kafkaBrokerServer;

    @ConfigProperty(name = "kafkaBrokerPort")
    String kafkaBrokerPort;

    @ConfigProperty(name = "quarkus.kafka-streams.bootstrap-servers")
    String bootstrapServers;

    @ConfigProperty(name = "zipkin.endpoint")
    String zipkinEndpoint;

    private KafkaStreamsTracing kafkaStreamsTracing;
    private KafkaStreams streams;

    void onStart(@Observes StartupEvent event) {
        logEnvironmentVariables();
        String xslt = createXsltLoader().loadXslt(xsltName);
        XsltTransformer.build(xslt);

        if (shouldUseTracing())
            kafkaStreamsTracing = configureTracing();

        TopologyProducer topologyProducer = createTopologyProducer();
        Topology topology = topologyProducer.buildTopology();

        streams = new KafkaStreamsFactory(topology).createStream();
        streams.start();

        System.out.println("XSLT: ");
        System.out.println(xslt);

        LOGGER.log(INFO, "XSLT:");
        LOGGER.log(INFO, xslt);
    }

    private boolean shouldUseTracing() {
        if (zipkinEndpoint != null && !zipkinEndpoint.trim().isEmpty())
            return true;
        return false;
    }

    private KafkaStreamsTracing configureTracing() {
        AsyncReporter.builder(URLConnectionSender.create(zipkinEndpoint)).build();
        AsyncReporter<Span> asyncReporter = AsyncReporter.create(URLConnectionSender.create(zipkinEndpoint));
        Tracing tracing = Tracing.newBuilder()
                                 .localServiceName(appName)
                                 .sampler(Sampler.ALWAYS_SAMPLE)
                                 .spanReporter(asyncReporter)
                                 .build();
        return KafkaStreamsTracing.create(tracing);
    }

    private String buildBootstrapServers() {
        return kafkaBrokerServer + ":" + kafkaBrokerPort;
    }

    private XsltLoader createXsltLoader() {
        return new KafkaXsltLoader(buildBootstrapServers(), xsltKafkaTopic);
    }

    private TopologyProducer createTopologyProducer() {

        return new TopologyProducer(buildBootstrapServers(), appName, inputKafkaTopic, outputKafkaTopic);
    }

    private void logEnvironmentVariables() {
        LOGGER.log(INFO, "inputKafkaTopic: " + inputKafkaTopic);
        LOGGER.log(INFO, "outputKafkaTopic: " + outputKafkaTopic);
        LOGGER.log(INFO, "xsltKafkaTopic: " + xsltKafkaTopic);
        LOGGER.log(INFO, "xsltName: " + xsltName);
        LOGGER.log(INFO, "appName: " + appName);
        LOGGER.log(INFO, "kafkaBrokerServer: " + kafkaBrokerServer);
        LOGGER.log(INFO, "kafkaBrokerPort: " + kafkaBrokerPort);
    }

    private class KafkaStreamsFactory {
        private Topology topology;

        KafkaStreamsFactory(Topology topology) {
            this.topology = topology;
        }

        KafkaStreams createStream() {
            if (useOpenTracing())
                return new KafkaStreams(topology, createStreamingConfig());
            kafkaStreamsTracing = configureTracing();
            return kafkaStreamsTracing.kafkaStreams(topology, createStreamingConfig());
        }

        private boolean useOpenTracing() {
            return zipkinEndpoint == null || zipkinEndpoint.trim().isEmpty();
        }

        private Properties createStreamingConfig() {
            Properties streamingConfig = new Properties();
            streamingConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
            streamingConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            streamingConfig.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamingConfig.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
            streamingConfig.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
            return streamingConfig;
        }
    }
}
