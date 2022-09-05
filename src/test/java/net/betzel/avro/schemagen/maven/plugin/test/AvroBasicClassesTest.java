package net.betzel.avro.schemagen.maven.plugin.test;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroBasicClassesTest extends AbstractAvroTest implements Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroBasicClassesTest.class);

    @Test
    public void testAllowNullFields() throws IOException {
        ReflectData reflectData = new ReflectData.AllowNull();
        CarGarage carGarage = new CarGarage();
        Schema schema = reflectData.getSchema(CarGarage.class);
        LOGGER.info("Schema: {}", schema.toString(true));
        byte[] bytes = encode(reflectData, schema, carGarage);
        LOGGER.info("Size of serialized data in bytes: {}", bytes.length);
        carGarage = decode(reflectData, schema, bytes);
        Assert.assertNotNull(carGarage);
        Assert.assertTrue(carGarage.getCapacity() == 0);
        Assert.assertNull(carGarage.getName());
    }

    @Test(expected = java.lang.NullPointerException.class)
    public void testAllowNoNullFields() throws IOException {
        ReflectData reflectData = new ReflectData();
        CarGarage carGarage = new CarGarage();
        Schema schema = reflectData.getSchema(CarGarage.class);
        LOGGER.info("Schema: {}", schema.toString(true));
        encode(reflectData, schema, carGarage);
    }

    @Test
    public void testAllowDefaultNullFields() throws IOException {
        ReflectData reflectData = new ReflectData().setDefaultsGenerated(true);
        CarGarage carGarage = new CarGarage();
        carGarage.setName("My garage");
        Schema schema = reflectData.getSchema(CarGarage.class);
        Assert.assertTrue(schema.toString(true).contains("\"default\" : 0"));
        LOGGER.info("Schema: {}", schema.toString(true));
        byte[] bytes = encode(reflectData, schema, carGarage);
        LOGGER.info("Size of serialized data in bytes: {}", bytes.length);
        carGarage = decode(reflectData, schema, bytes);
        Assert.assertNotNull(carGarage);
        Assert.assertTrue(carGarage.getCapacity() == 0);
        Assert.assertTrue(carGarage.getName().equals("My garage"));
    }

}