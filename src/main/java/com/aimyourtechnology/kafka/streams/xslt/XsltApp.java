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
        TopologyProducer topologyProducer = createTopologyProducer(xslt);
        topologyProducer.runTopology();

        System.out.println("XSLT: ");
        System.out.println(xslt);

        LOGGER.log(INFO, "XSLT:");
        LOGGER.log(INFO, xslt);
    }

//    @Incoming("XSLT")
//    public void loadXslt(String xslt) {
//
//    }

    private String buildBootstrapServers() {
        return kafkaBrokerServer + ":" + kafkaBrokerPort;
    }

    private XsltLoader createXsltLoader() {
        return new KafkaXsltLoader(buildBootstrapServers(), xsltKafkaTopic);
    }

    private TopologyProducer createTopologyProducer(String xslt) {
        return new TopologyProducer(buildBootstrapServers(), appName, xslt, inputKafkaTopic, outputKafkaTopic);
    }

    private void logEnvironmentVariables() {
        System.out.println("inputKafkaTopic: " + inputKafkaTopic);
        System.out.println("outputKafkaTopic: " + outputKafkaTopic);
        System.out.println("xsltKafkaTopic: " + xsltKafkaTopic);
        System.out.println("xsltName: " + xsltName);
        System.out.println("appName: " + appName);
        System.out.println("kafkaBrokerServer: " + kafkaBrokerServer);
        System.out.println("kafkaBrokerPort: " + kafkaBrokerPort);
    }

}
