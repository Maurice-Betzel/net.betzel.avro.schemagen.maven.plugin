package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroEncoderDecoder;
import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

public class PolymorphicClassesTest {

    private static Logger LOGGER = LoggerFactory.getLogger(PolymorphicClassesTest.class);

    private static Cars generateData() {

        Cars carCollection = new Cars();
        carCollection.name = "Car collection";
        carCollection.cars = new ArrayList();

        HotHatchback hotHatchback = new HotHatchback();
        hotHatchback.color = "BLUE";
        hotHatchback.horsepower = 200.5;
        carCollection.cars.add(hotHatchback);

        Convertible convertible = new Convertible();
        convertible.color = "RED";
        convertible.passengerCapacity = 2;
        carCollection.cars.add(convertible);

        return carCollection;
    }

    @Test
    public void testPolymorphicClasses() throws IOException {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
        Schema carCollectionSchema = schemaGenerator.generateSchema(Cars.class);
        LOGGER.info("Schema without subtypes: {}", carCollectionSchema.toString(true));

        schemaGenerator.declarePolymorphicType(HotHatchback.class, Convertible.class);
        carCollectionSchema = schemaGenerator.generateSchema(Cars.class);
        LOGGER.info("Schema with subtypes added: {}", carCollectionSchema.toString(true));

        Cars departureQueue = generateData();
        AvroEncoderDecoder<Cars> serializer = new AvroEncoderDecoder(carCollectionSchema);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        serializer.encodeAvro(byteArrayOutputStream, departureQueue);
        LOGGER.info("Size of serialized data in bytes: {}", byteArrayOutputStream.toByteArray().length);
        Cars carCollectionSchemaRestored = serializer.decodeAvro(new ByteArrayInputStream(byteArrayOutputStream.toByteArray()));
        Assert.assertTrue(carCollectionSchemaRestored.cars.get(0).color.equals("BLUE"));
        Assert.assertTrue(((HotHatchback) (carCollectionSchemaRestored.cars.get(0))).horsepower == 200.5);
        Assert.assertTrue(carCollectionSchemaRestored.cars.get(1).color.equals("RED"));
        Assert.assertTrue(((Convertible) (carCollectionSchemaRestored.cars.get(1))).passengerCapacity == 2);
    }

}