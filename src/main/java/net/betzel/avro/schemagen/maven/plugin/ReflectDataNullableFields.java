package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * {@link ReflectData} implementation that permits null field values. The schema
 * generated for each field is a union of its declared type and null.
 * Skipping polymorph fields of array and map if applicable
 */

public class ReflectDataNullableFields extends ReflectData {

    private AvroSchemaGenerator avroSchemaGenerator;

    public ReflectDataNullableFields(AvroSchemaGenerator avroSchemaGenerator) {
        this.avroSchemaGenerator = avroSchemaGenerator;
    }

    /**
     * Create and return a union of the null schema and the provided schema.
     */
    public static Schema makeNullable(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            // check to see if the union already contains NULL
            for (Schema subType : schema.getTypes()) {
                if (subType.getType() == Schema.Type.NULL) {
                    return schema;
                }
            }
            // add null as the first type in a new union
            List<Schema> withNull = new ArrayList<>();
            withNull.add(Schema.create(Schema.Type.NULL));
            withNull.addAll(schema.getTypes());
            return Schema.createUnion(withNull);
        } else {
            // create a union with null
            return Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL), schema));
        }
    }

    @Override
    protected Schema createFieldSchema(Field field, Map<String, Schema> names) {
        Schema schema = super.createFieldSchema(field, names);
        if (field.getType().isPrimitive()) {
            // for primitive values a null will result in a NullPointerException at read time
            return schema;
        }
        if (avroSchemaGenerator.hasPolymorphicTypeSchemas()) {
            // let the polymorph schema generator deal with these null types
            if (schema.getType().equals(Schema.Type.ARRAY)) {
                return schema;
            }
            if (schema.getType().equals(Schema.Type.MAP)) {
                return schema;
            }
        }
        return makeNullable(schema);
    }

}