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

public class AvroMixedTypesTest extends AbstractAvroTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroMixedTypesTest.class);

    @Test
    public void testMixedTypesAllowNullFields() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
        LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
        AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
        byte[] avroMixedTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroMixedTypesRecordBytes.length);
        AvroMixedTypesRecord avroMixedTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecordBytes);
        Diff diff = javers.compare(avroMixedTypesRecord, avroMixedTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testMixedTypesAllowNoneNullFields() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
        LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
        AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
        byte[] avroMixedTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroMixedTypesRecordBytes.length);
        AvroMixedTypesRecord avroMixedTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecordBytes);
        Diff diff = javers.compare(avroMixedTypesRecord, avroMixedTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

}