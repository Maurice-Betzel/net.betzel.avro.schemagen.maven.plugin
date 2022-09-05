package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroSchemaGenerator;
import org.apache.avro.Schema;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;

public class AvroPolymorphicClassesTest extends AbstractAvroTest implements Serializable {

    private static Logger LOGGER = LoggerFactory.getLogger(AvroPolymorphicClassesTest.class);

    @Test
    public void testPolymorphicClasses2() {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(true);
        schemaGenerator.declarePolymorphicType(CarHotHatchback.class, CarConvertible.class);
        Schema carGarageSchema = schemaGenerator.generateSchema(CarGarage.class);
        LOGGER.info("Schema with subtypes added: {}", carGarageSchema.toString(true));
    }


    @Test
    public void testPolymorphicClasses() throws IOException {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(false);
//        Schema carCollectionSchema = schemaGenerator.generateSchema(Cars.class);
//        LOGGER.info("Schema without subtypes: {}", carCollectionSchema.toString(true));

        schemaGenerator.declarePolymorphicType(CarHotHatchback.class, CarConvertible.class);
        Schema carCollectionSchema = schemaGenerator.generateSchema(Cars.class);
        LOGGER.info("Schema with subtypes added: {}", carCollectionSchema.toString(true));

//        Cars carCollection = new Cars();
//        carCollection.name = "Car collection";
//        carCollection.cars = new ArrayList();
//
//        CarConvertible carConvertible = new CarConvertible();
//        carConvertible.color = "RED";
//        carConvertible.passengerCapacity = 2;
//        carCollection.cars.add(carConvertible);
//
//        CarHotHatchback carHotHatchback = new CarHotHatchback();
//        carHotHatchback.color = "BLUE";
//        carHotHatchback.horsepower = 200.5;
//        carHotHatchback.carRims = new CarRims();
//        carHotHatchback.carRims.setHasExtraWide(true);
//        carHotHatchback.carRims.setInches(20.0f);
//        carCollection.cars.add(carHotHatchback);
//
//        byte[] bytes = encode(schemaGenerator.getReflectData(), carCollectionSchema, carCollection);
//        LOGGER.info("Size of serialized data in bytes: {}", bytes.length);
//        Cars carCollectionSchemaRestored = decode(schemaGenerator.getReflectData(), carCollectionSchema, bytes);
//
//        Assert.assertTrue(carCollectionSchemaRestored.cars.get(0).color.equals("RED"));
//        Assert.assertTrue(((CarConvertible) (carCollectionSchemaRestored.cars.get(0))).passengerCapacity == 2);
//        Assert.assertTrue(carCollectionSchemaRestored.cars.get(1).color.equals("BLUE"));
//        Assert.assertTrue(((CarHotHatchback) (carCollectionSchemaRestored.cars.get(1))).horsepower == 200.5);
    }

    @Test
    public void testPolymorphicCarGarage() throws IOException {
        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(true);
        schemaGenerator.declarePolymorphicType(CarHotHatchback.class, CarConvertible.class);
        Schema polymorphicCarGarage = schemaGenerator.generateSchema(CarGarage.class);
        LOGGER.info("Schema with subtypes added: {}", polymorphicCarGarage.toString(true));

    }


}