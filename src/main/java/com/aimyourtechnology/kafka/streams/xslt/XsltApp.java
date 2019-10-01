package com.aimyourtechnology.kafka.streams.xslt;

import io.quarkus.runtime.StartupEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.reactive.messaging.Incoming;

import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.event.Observes;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.util.logging.Level.*;

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

    void onStart(@Observes StartupEvent event) {
        logEnvironmentVariables();
        String xslt = createXsltLoader().loadXslt(xsltName);
        XsltTransformer.build(xslt);
        TopologyProducer topologyProducer = createTopologyProducer();
        topologyProducer.runTopology();

        System.out.println("XSLT: ");
        System.out.println(xslt);

        LOGGER.log(INFO, "XSLT:");
        LOGGER.log(INFO, xslt);
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

}
