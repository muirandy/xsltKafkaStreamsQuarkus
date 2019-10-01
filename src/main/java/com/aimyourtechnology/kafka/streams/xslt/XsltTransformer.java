package com.aimyourtechnology.kafka.streams.xslt;

import javax.xml.transform.*;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import java.io.*;

class XsltTransformer {
    private static final String CHARSET = "UTF-8";
    private static Templates CACHED_XSLT;

    static void build(String xslt) {
        try {
            InputStream inputStream = new ByteArrayInputStream(xslt.getBytes(CHARSET));
            Source xsltSource = new StreamSource(inputStream);
            TransformerFactory transFact = TransformerFactory.newInstance();
            CACHED_XSLT = transFact.newTemplates(xsltSource);
        } catch (TransformerConfigurationException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new UnableToCompileXsltException(e);
        }
    }

    static String transform(String incomingString) {
        try {
            InputStream inputStream = new ByteArrayInputStream(incomingString.getBytes(CHARSET));
            StreamSource incomingStreamSource = new StreamSource(inputStream);
            Transformer transformer = CACHED_XSLT.newTransformer();
            OutputStream outputStream = new ByteArrayOutputStream();
            Result result = new StreamResult(outputStream);
            transformer.transform(incomingStreamSource, result);
            return outputStream.toString();
        } catch (TransformerException | UnsupportedEncodingException e) {
            e.printStackTrace();
            throw new UnableToCompileXsltException(e);
        }
    }

    private static class UnableToCompileXsltException extends RuntimeException {
        public UnableToCompileXsltException(Exception e) {
            super(e);
        }
    }
}
