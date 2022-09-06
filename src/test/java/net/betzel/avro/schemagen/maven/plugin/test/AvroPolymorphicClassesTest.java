package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.javers.core.diff.Diff;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroPolymorphicClassesTest extends AbstractAvroTest implements Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroPolymorphicClassesTest.class);

    @Test
    public void testPolymorphicTypesAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class);
        Schema avroPolymorphicRecordSchema = avroSchemaGenerator.generateSchema(AvroPolymorphicTypesRecord.class);
        LOGGER.info("Polymorphic types schema: {}", avroPolymorphicRecordSchema.toString(true));
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecord = new AvroPolymorphicTypesRecord();
        avroPolymorphicTypesRecord.exception = new IOException("IO Exception");
        avroPolymorphicTypesRecord.runtimeException = new NullPointerException("Nullpointer Exception");
        byte[] avroPolymorphicTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroPolymorphicTypesRecordBytes.length);
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecordBytes);
        Diff diff = javers.compare(avroPolymorphicTypesRecord, avroPolymorphicTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

}