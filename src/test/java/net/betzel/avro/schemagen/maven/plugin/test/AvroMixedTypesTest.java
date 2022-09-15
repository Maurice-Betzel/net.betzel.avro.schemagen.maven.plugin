package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.UnresolvedUnionException;
import org.javers.core.diff.Diff;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class AvroMixedTypesTest extends AbstractAvroTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroMixedTypesTest.class);

    @Test
    public void testMixedTypesAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(Properties.class, IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
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
    public void testMixedTypesAllowNullFields2() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
        LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
        AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
        avroMixedTypesRecord.date = null;
        avroMixedTypesRecord.throwable = null;
        byte[] avroMixedTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroMixedTypesRecordBytes.length);
        AvroMixedTypesRecord avroMixedTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecordBytes);
        Diff diff = javers.compare(avroMixedTypesRecord, avroMixedTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testMixedTypesAllowNullFields3() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
        LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
        AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
        avroMixedTypesRecord.exceptions = null;
        avroMixedTypesRecord.stringDateHashMap = null;
        byte[] avroMixedTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroMixedTypesRecordBytes.length);
        AvroMixedTypesRecord avroMixedTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecordBytes);
        Diff diff = javers.compare(avroMixedTypesRecord, avroMixedTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testMixedTypesAllowNoneNullFields1() throws IOException {
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

    @Test
    public void testMixedTypesDisallowNullFieldsException1() {
        UnresolvedUnionException unresolvedUnionException = Assert.assertThrows(UnresolvedUnionException.class, () -> {
            AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false, false, false);
            avroSchemaGenerator.setConversions(conversions);
            avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
            Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
            LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
            AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
            avroMixedTypesRecord.date = null;
            encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        });
        Assert.assertTrue(unresolvedUnionException.getMessage().contains("Not in union [{\"type\":\"long\",\"logicalType\":\"util-date-timestamp-millis\"}]: null (field=date)"));
    }

    @Test
    public void testMixedTypesDisallowNullFieldsException2() {
        UnresolvedUnionException unresolvedUnionException = Assert.assertThrows(UnresolvedUnionException.class, () -> {
            AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false, false, false);
            avroSchemaGenerator.setConversions(conversions);
            avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
            Schema avroMixedTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroMixedTypesRecord.class);
            LOGGER.info("Mixed types schema with null: {}", avroMixedTypesRecordSchema.toString(true));
            AvroMixedTypesRecord avroMixedTypesRecord = new AvroMixedTypesRecord(random, localDateTime);
            avroMixedTypesRecord.exceptions = null;
            encode(avroSchemaGenerator.getReflectData(), avroMixedTypesRecordSchema, avroMixedTypesRecord);
        });
        Assert.assertTrue(unresolvedUnionException.getMessage().contains("Not in union [{\"type\":\"array\",\"items\":[{\"type\":\"error\",\"name\":\"IOException\",\"namespace\":\"java.io\",\"fields\":[{\"name\":\"detailMessage\"," +
                "\"type\":[\"null\",\"string\"],\"default\":null}]},{\"type\":\"error\",\"name\":\"ArrayIndexOutOfBoundsException\",\"namespace\":\"java.lang\",\"fields\":[{\"name\":\"detailMessage\",\"type\":[\"null\",\"string\"]," +
                "\"default\":null}]},{\"type\":\"error\",\"name\":\"Exception\",\"namespace\":\"java.lang\",\"fields\":[{\"name\":\"detailMessage\",\"type\":[\"null\",\"string\"],\"default\":null}]},{\"type\":\"error\"," +
                "\"name\":\"IllegalArgumentException\",\"namespace\":\"java.lang\",\"fields\":[{\"name\":\"detailMessage\",\"type\":[\"null\",\"string\"],\"default\":null}]},{\"type\":\"error\"," +
                "\"name\":\"InterruptedException\",\"namespace\":\"java.lang\",\"fields\":[{\"name\":\"detailMessage\",\"type\":[\"null\",\"string\"],\"default\":null}]},{\"type\":\"error\"," +
                "\"name\":\"NullPointerException\",\"namespace\":\"java.lang\",\"fields\":[{\"name\":\"detailMessage\",\"type\":[\"null\",\"string\"],\"default\":null}]}]," +
                "\"java-class\":\"java.util.List\"}]: null (field=exceptions)"));
    }

}