package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroConversions;
import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroSchemaEvolutionTest extends AbstractAvroTest implements Serializable {

    // backward compatible schema evolution as in semantic versioning
    // BACKWARD compatibility means facades promoting and using the new schema can read data produced with the last schema

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaEvolutionTest.class);

    private ReflectData reflectData = ReflectData.AllowNull.get();

    static AvroSchemaEvolutionInterface decode(ReflectData reflectData, Schema writer, Schema reader, byte[] bytes) throws IOException {
        DatumReader<AvroSchemaEvolutionInterface> datumReader = reflectData.createDatumReader(writer, reader);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return datumReader.read(null, binaryDecoder);
    }

    @Before
    public void init() {
        reflectData.addLogicalTypeConversion(new AvroConversions.UtilDateTimestampMillis());
    }

    @Test
    public void testSchemaEvolution() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        Schema avroSchemaEvolutionOldRecordSchema = avroSchemaGenerator.generateSchema(AvroSchemaEvolutionOldRecord.class);
        LOGGER.info("Old schema: {}", avroSchemaEvolutionOldRecordSchema.toString(true));
        AvroSchemaEvolutionInterface avroSchemaEvolutionOldRecord = new AvroSchemaEvolutionOldRecord();
        avroSchemaEvolutionOldRecord.setDate(localDateTime);
        byte[] avroSchemaEvolutionOldRecordBytes = encode(reflectData, avroSchemaEvolutionOldRecordSchema, avroSchemaEvolutionOldRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroSchemaEvolutionOldRecordBytes.length);
        Schema avroSchemaEvolutionNewRecordSchema = avroSchemaGenerator.generateSchema(AvroSchemaEvolutionNewRecord.class);
        LOGGER.info("New schema: {}", avroSchemaEvolutionNewRecordSchema.toString(true));
        AvroSchemaEvolutionInterface avroSchemaEvolutionNewRecord = decode(reflectData, avroSchemaEvolutionOldRecordSchema, avroSchemaEvolutionNewRecordSchema, avroSchemaEvolutionOldRecordBytes);
        Assert.assertTrue(avroSchemaEvolutionOldRecord.getDate().equals(avroSchemaEvolutionNewRecord.getDate()));
        AvroSchemaEvolutionNewRecord AvroSchemaEvolutionNewRecord = (net.betzel.avro.schemagen.maven.plugin.test.AvroSchemaEvolutionNewRecord) avroSchemaEvolutionNewRecord;
        Assert.assertNull(AvroSchemaEvolutionNewRecord.dateString);
    }

}