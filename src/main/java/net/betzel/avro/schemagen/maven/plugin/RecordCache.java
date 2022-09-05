package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;
import org.apache.avro.specific.SpecificData;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

// A cache of record schemas
public class RecordCache {

    private final Map<String, Schema> cachedRecords;
    private final Set<String> emptyCachedRecords;

    public RecordCache() {
        cachedRecords = new HashMap();
        emptyCachedRecords = new HashSet();
    }

    public void clear() {
        cachedRecords.clear();
        emptyCachedRecords.clear();
    }

    // Create a schema for template
    public Schema create(Schema template) {
        if (template.getType() != Schema.Type.RECORD) {
            String message = String.format("Attempted to cache non-Record schema of type %s: %s", template.getType(), template.getFullName());
            throw new SchemaGenerationException(message);
        }
        String fullName = template.getFullName();
        if (cachedRecords.containsKey(fullName)) {
            String message = String.format("Attempted to cache schema for %s but it already exists", fullName);
            throw new SchemaGenerationException(message);
        }
        Schema newSchema = Schema.createRecord(template.getName(), template.getDoc(), template.getNamespace(), template.isError());
        cachedRecords.put(fullName, newSchema);
        emptyCachedRecords.add(SpecificData.getClassName(template));
        return newSchema;
    }

    public Schema get(Schema schema) {
        return get(schema.getFullName());
    }

    public Schema get(String fullName) {
        return cachedRecords.get(fullName);
    }

    // Get a schema for s or create it
    public Schema getOrCreate(Schema schema) {
        Schema newSchema = get(schema);
        if (Objects.isNull(newSchema)) {
            newSchema = create(schema);
        }
        return newSchema;
    }

    // Set the fields for the schema
    public Schema set(Schema schema, List<Schema.Field> fields) {
        String fullName = SpecificData.getClassName(schema);
        Schema newSchema = getOrCreate(schema);
        if (!emptyCachedRecords.contains(fullName)) {
            String errMsg = "Fields have already been set for " + fullName;
            throw new SchemaGenerationException(errMsg);
        }
        newSchema.setFields(fields);
        emptyCachedRecords.remove(fullName);
        return newSchema;
    }

    // Return whether fields are set for s
    public boolean isFieldsSet(Schema schema) {
        return isComplete(schema.getFullName());
    }

    public boolean isComplete(String fullName) {
        return cachedRecords.containsKey(fullName) && (!emptyCachedRecords.contains(fullName));
    }

    public String getSchemaWithoutFields() {
        for (String string : emptyCachedRecords) {
            return string;
        }
        return null;
    }
}
