package com.aimyourtechnology.kafka.streams.xslt;

import org.junit.jupiter.api.Test;
import org.xmlunit.assertj.XmlAssert;

import java.util.UUID;

public class XsltTransformerTest {
    private String randomValue = UUID.randomUUID().toString();

    @Test
    void xmlIsTransformed() {
        XsltTransformer.build(createXslt());

        String result = XsltTransformer.transform(createXml());

        XmlAssert.assertThat(result).and(createTransformedXmlMessage())
                 .ignoreWhitespace()
                 .areIdentical();

    }

    private String createXslt() {
        return "<xsl:stylesheet version=\"1.0\" xmlns:xsl=\"http://www.w3.org/1999/XSL/Transform\">\n" +
                "    <xsl:template match=\"/\">\n" +
                "        <order>\n" +
                "            <xsl:attribute name=\"id\">\n" +
                "                <xsl:value-of select=\"order/orderId\"/>\n" +
                "            </xsl:attribute>\n" +
                "        </order>\n" +
                "    </xsl:template>\n" +
                "</xsl:stylesheet>";
    }

    private String createXml() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order>" +
                        "<orderId>%s</orderId>\n" +
                        "</order>", randomValue
        );
    }

    private String createTransformedXmlMessage() {
        return String.format(
                "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                        "<order id=\"%s\"/>",
                randomValue
        );
    }


}
