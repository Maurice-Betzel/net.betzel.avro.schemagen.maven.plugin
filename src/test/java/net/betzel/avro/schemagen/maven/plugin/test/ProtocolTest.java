package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Protocol;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProtocolTest {

    private static Logger LOGGER = LoggerFactory.getLogger(ProtocolTest.class);

    @Test
    public void testProtocolClasses() throws IOException {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
        Protocol protocol = schemaGenerator.generateProtocol(ProtocolTestInterface.class);
        LOGGER.info("Protocol without subtypes: {}", protocol.toString(true));

        schemaGenerator = new AvroSchemaGenerator();
        schemaGenerator.declarePolymorphicType(HotHatchback.class, Convertible.class);
        protocol = schemaGenerator.generateProtocol(ProtocolTestInterface.class);
        LOGGER.info("Protocol with subtypes added: {}", protocol.toString(true));
    }

}