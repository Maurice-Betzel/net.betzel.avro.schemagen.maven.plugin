package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroConversions;
import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.reflect.ReflectData;
import org.javers.core.diff.Diff;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroInheritTypesTest extends AbstractAvroTest implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroInheritTypesTest.class);

    private ReflectData reflectData = ReflectData.AllowNull.get();

    @Before
    public void init() {
        reflectData.addLogicalTypeConversion(new TimeConversions.DateConversion());
        reflectData.addLogicalTypeConversion(new TimeConversions.TimeMillisConversion());
        reflectData.addLogicalTypeConversion(new AvroConversions.UtilDateTimestampMillis());
    }

    @Test
    public void testMixedTypesAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        Schema avroInheritChildTypesRecordSchema = avroSchemaGenerator.generateSchema(AvroInheritChild2TypesRecord.class);
        LOGGER.info("Inherited child type schema with null: {}", avroInheritChildTypesRecordSchema.toString(true));
        AvroInheritChild2TypesRecord AvroInheritChild2TypesRecord = new AvroInheritChild2TypesRecord(localDateTime);
        byte[] avroInheritChildTypesRecordBytes = encode(reflectData, avroInheritChildTypesRecordSchema, AvroInheritChild2TypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroInheritChildTypesRecordBytes.length);
        AvroInheritChild2TypesRecord AvroInheritChild2TypesRecordRestored = decode(reflectData, avroInheritChildTypesRecordSchema, avroInheritChildTypesRecordBytes);
        Diff diff = javers.compare(AvroInheritChild2TypesRecord, AvroInheritChild2TypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

}