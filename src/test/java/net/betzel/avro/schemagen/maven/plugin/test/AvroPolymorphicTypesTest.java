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
import java.util.ArrayList;
import java.util.List;

public class AvroPolymorphicTypesTest extends AbstractAvroTest implements Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroPolymorphicTypesTest.class);

    @Test
    public void testPolymorphicTypesAllowNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(true, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroPolymorphicRecordSchema = avroSchemaGenerator.generateSchema(AvroPolymorphicTypesRecord.class);
        LOGGER.info("Polymorphic types schema allow null: {}", avroPolymorphicRecordSchema.toString(true));
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecord = new AvroPolymorphicTypesRecord();
        avroPolymorphicTypesRecord.throwable = new IllegalArgumentException("Illegal Argument Exception");
        avroPolymorphicTypesRecord.exception = new IOException("IO Exception");
        avroPolymorphicTypesRecord.runtimeException = new NullPointerException("Null pointer Exception");
        List<Throwable> throwables = new ArrayList(5);
        throwables.add(new IOException("1"));
        throwables.add(new NullPointerException("2"));
        throwables.add(new InterruptedException("3"));
        throwables.add(new IllegalArgumentException("4"));
        throwables.add(new ArrayIndexOutOfBoundsException(5));
        avroPolymorphicTypesRecord.throwables = throwables;
        List<Exception> exceptions = new ArrayList(5);
        exceptions.add(new IOException("1"));
        exceptions.add(new NullPointerException("2"));
        exceptions.add(new InterruptedException("3"));
        exceptions.add(new IllegalArgumentException("4"));
        exceptions.add(new ArrayIndexOutOfBoundsException(5));
        avroPolymorphicTypesRecord.exceptions = exceptions;
        List<Serializable> serializables = new ArrayList(5);
        serializables.add(new IOException("1"));
        serializables.add(new NullPointerException("2"));
        serializables.add(new InterruptedException("3"));
        serializables.add(new IllegalArgumentException("4"));
        serializables.add(new ArrayIndexOutOfBoundsException(5));
        //avroPolymorphicTypesRecord.serializables = serializables;
        byte[] avroPolymorphicTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroPolymorphicTypesRecordBytes.length);
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecordBytes);
        Diff diff = javers.compare(avroPolymorphicTypesRecord, avroPolymorphicTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

    @Test
    public void testPolymorphicTypesAllowNonNullFields1() throws IOException {
        AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(false, false, false);
        avroSchemaGenerator.setConversions(conversions);
        avroSchemaGenerator.declarePolymorphicType(IllegalArgumentException.class, NullPointerException.class, IOException.class, InterruptedException.class, ArrayIndexOutOfBoundsException.class);
        Schema avroPolymorphicRecordSchema = avroSchemaGenerator.generateSchema(AvroPolymorphicTypesRecord.class);
        LOGGER.info("Polymorphic types schema without null: {}", avroPolymorphicRecordSchema.toString(true));
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecord = new AvroPolymorphicTypesRecord();
        avroPolymorphicTypesRecord.throwable = new IllegalArgumentException("Illegal Argument Exception");
        avroPolymorphicTypesRecord.exception = new IOException("IO Exception");
        avroPolymorphicTypesRecord.runtimeException = new NullPointerException("Null pointer Exception");
        List<Throwable> throwables = new ArrayList(5);
        throwables.add(new IOException("1"));
        throwables.add(new NullPointerException("2"));
        throwables.add(new InterruptedException("3"));
        throwables.add(new IllegalArgumentException("4"));
        throwables.add(new ArrayIndexOutOfBoundsException(5));
        avroPolymorphicTypesRecord.throwables = throwables;
        List<Exception> exceptions = new ArrayList(5);
        exceptions.add(new IOException("1"));
        exceptions.add(new NullPointerException("2"));
        exceptions.add(new InterruptedException("3"));
        exceptions.add(new IllegalArgumentException("4"));
        exceptions.add(new ArrayIndexOutOfBoundsException(5));
        avroPolymorphicTypesRecord.exceptions = exceptions;
        List<Serializable> serializables = new ArrayList(5);
        serializables.add(new IOException("1"));
        serializables.add(new NullPointerException("2"));
        serializables.add(new InterruptedException("3"));
        serializables.add(new IllegalArgumentException("4"));
        serializables.add(new ArrayIndexOutOfBoundsException(5));
        //avroPolymorphicTypesRecord.serializables = serializables;
        byte[] avroPolymorphicTypesRecordBytes = encode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecord);
        LOGGER.info("Size of serialized data in bytes: {}", avroPolymorphicTypesRecordBytes.length);
        AvroPolymorphicTypesRecord avroPolymorphicTypesRecordRestored = decode(avroSchemaGenerator.getReflectData(), avroPolymorphicRecordSchema, avroPolymorphicTypesRecordBytes);
        Diff diff = javers.compare(avroPolymorphicTypesRecord, avroPolymorphicTypesRecordRestored);
        Assert.assertFalse(diff.hasChanges());
    }

}