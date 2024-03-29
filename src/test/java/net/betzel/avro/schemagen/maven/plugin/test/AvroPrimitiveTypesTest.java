package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.javers.core.diff.Diff;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroPrimitiveTypesTest extends AbstractAvroTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroPrimitiveTypesTest.class);

    private ReflectData reflectData = ReflectData.get();

    @Test
    public void testPrimitivesAllowNullFields() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        Schema avroPrimitiveTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroPrimitiveTypesRecord.class);
        LOGGER.info("Primitive types schema with null: {}", avroPrimitiveTypesRecordSchema.toString(true));
        AvroPrimitiveTypesRecord avroPrimitiveTypesRecord = new AvroPrimitiveTypesRecord(random);
        byte[] avroPrimitiveTypesRecordBytes = encode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroPrimitiveTypesRecordBytes.length);
        AvroPrimitiveTypesRecord avroPrimitiveTypesRecordRestored = decode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecordBytes);
        Diff diff = javers.compare(avroPrimitiveTypesRecord, avroPrimitiveTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
        Assert.assertNull(avroPrimitiveTypesRecordRestored.s1);
        Assert.assertNull(avroPrimitiveTypesRecordRestored.nullBytes);
    }

    @Test
    public void testPrimitivesDisallowNullFields() throws IOException {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(false, false, false);
        schemaGenerator.setConversions(conversions);
        Schema avroPrimitiveTypesRecordSchema = schemaGenerator.generateSchema(AvroPrimitiveTypesRecord.class);
        LOGGER.info("Primitive types schema without null: {}", avroPrimitiveTypesRecordSchema.toString(true));
        AvroPrimitiveTypesRecord avroPrimitiveTypesRecord = new AvroPrimitiveTypesRecord(random);
        avroPrimitiveTypesRecord.s1 = "Avoid null pointer on S1";
        avroPrimitiveTypesRecord.nullBytes = new byte[]{};
        byte[] avroPrimitiveTypesRecordBytes = encode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroPrimitiveTypesRecordBytes.length);
        AvroPrimitiveTypesRecord avroPrimitiveTypesRecordRestored = decode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecordBytes);
        Diff diff = javers.compare(avroPrimitiveTypesRecord, avroPrimitiveTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
        Assert.assertNotNull(avroPrimitiveTypesRecordRestored.s1);
        Assert.assertNotNull(avroPrimitiveTypesRecordRestored.nullBytes);
    }

    @Test
    public void testPrimitivesDisallowNullFieldsException1() {
        NullPointerException nullPointerException = Assert.assertThrows(NullPointerException.class, () -> {
            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(false, false, false);
            schemaGenerator.setConversions(conversions);
            Schema avroPrimitiveTypesRecordSchema = schemaGenerator.generateSchema(AvroPrimitiveTypesRecord.class);
            LOGGER.info("Primitive types schema without null: {}", avroPrimitiveTypesRecordSchema.toString(true));
            AvroPrimitiveTypesRecord avroPrimitiveTypesRecord = new AvroPrimitiveTypesRecord(random);
            encode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecord);
        });
        Assert.assertTrue(nullPointerException.getCause().getMessage().contains("null in bytes in field nullBytes"));
    }

    @Test
    public void testPrimitivesDisallowNullFieldsException2() {
        NullPointerException nullPointerException = Assert.assertThrows(NullPointerException.class, () -> {
            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(false, false, false);
            schemaGenerator.setConversions(conversions);
            Schema avroPrimitiveTypesRecordSchema = schemaGenerator.generateSchema(AvroPrimitiveTypesRecord.class);
            LOGGER.info("Primitive types schema without null: {}", avroPrimitiveTypesRecordSchema.toString(true));
            AvroPrimitiveTypesRecord avroPrimitiveTypesRecord = new AvroPrimitiveTypesRecord(random);
            avroPrimitiveTypesRecord.nullBytes = new byte[]{(byte) random.nextInt(), (byte) random.nextInt(), (byte) random.nextInt(), (byte) random.nextInt()};
            encode(reflectData, avroPrimitiveTypesRecordSchema, avroPrimitiveTypesRecord);
        });
        Assert.assertTrue(nullPointerException.getCause().getMessage().contains("null in string in field s1"));
    }

}