package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.AvroTypeException;
import org.apache.avro.Schema;
import org.javers.core.diff.Diff;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroComplexTypesTest extends AbstractAvroTest implements Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroComplexTypesTest.class);

    @Test
    public void testComplexTypesAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true);
        Schema avroComplexTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroComplexTypesRecord.class);
        LOGGER.info("Complex types schema: {}", avroComplexTypesRecordSchema.toString(true));
        AvroComplexTypesRecord avroComplexTypesRecord = new AvroComplexTypesRecord(random, localDateTime, zonedDateTime);
        byte[] avroComplexTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroComplexTypesRecordBytes.length);
        AvroComplexTypesRecord avroComplexTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecordBytes);
        Diff diff = javers.compare(avroComplexTypesRecord, avroComplexTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testComplexTypesAllowNullFields2() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true);
        Schema avroComplexTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroComplexTypesRecord.class);
        LOGGER.info("Complex types schema: {}", avroComplexTypesRecordSchema.toString(true));
        AvroComplexTypesRecord avroComplexTypesRecord = new AvroComplexTypesRecord(random, localDateTime, zonedDateTime);
        avroComplexTypesRecord.day = null;
        avroComplexTypesRecord.integerSet = null;
        byte[] avroComplexTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroComplexTypesRecordBytes.length);
        AvroComplexTypesRecord avroComplexTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecordBytes);
        Diff diff = javers.compare(avroComplexTypesRecord, avroComplexTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testComplexTypesDisAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false);
        Schema avroComplexTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroComplexTypesRecord.class);
        LOGGER.info("Complex types schema: {}", avroComplexTypesRecordSchema.toString(true));
        AvroComplexTypesRecord avroComplexTypesRecord = new AvroComplexTypesRecord(random, localDateTime, zonedDateTime);
        byte[] avroComplexTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroComplexTypesRecordBytes.length);
        AvroComplexTypesRecord avroComplexTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecordBytes);
        Diff diff = javers.compare(avroComplexTypesRecord, avroComplexTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testComplexTypesDisAllowNullFields2() {
        NullPointerException nullPointerException = Assert.assertThrows(NullPointerException.class, () -> {
            AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false);
            Schema avroComplexTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroComplexTypesRecord.class);
            LOGGER.info("Complex types schema: {}", avroComplexTypesRecordSchema.toString(true));
            AvroComplexTypesRecord avroComplexTypesRecord = new AvroComplexTypesRecord(random, localDateTime, zonedDateTime);
            avroComplexTypesRecord.integerSet = null;
            encode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecord);
        });
        Assert.assertTrue(nullPointerException.getCause().getMessage().contains("null in array in field integerSet"));
    }

    @Test
    public void testComplexTypesDisAllowNullFields3() {
        AvroTypeException avroTypeException = Assert.assertThrows(AvroTypeException.class, () -> {
            AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false);
            Schema avroComplexTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroComplexTypesRecord.class);
            LOGGER.info("Complex types schema: {}", avroComplexTypesRecordSchema.toString(true));
            AvroComplexTypesRecord avroComplexTypesRecord = new AvroComplexTypesRecord(random, localDateTime, zonedDateTime);
            avroComplexTypesRecord.day = null;
            encode(avroSchemaGenerator.getReflectData(), avroComplexTypesRecordSchema, avroComplexTypesRecord);
        });
        Assert.assertTrue(avroTypeException.getCause().getMessage().contains("value null is not a Day"));
    }

}