package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Conversion;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.util.ClassUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static net.betzel.avro.schemagen.maven.plugin.ReflectDataNullableFields.PRIMITIVE;
import static org.apache.avro.LogicalType.LOGICAL_TYPE_PROP;
import static org.apache.avro.Schema.Field.NULL_DEFAULT_VALUE;

public final class AvroSchemaGenerator {

    private static final Pattern uppercaseClassNamePattern = Pattern.compile("\\.[A-Z]");

    private final ReflectData reflectData;
    private final RecordCache recordCache = new RecordCache();
    private final Map<String, Schema> customSchemas = new HashMap();
    private final Map<String, Set<Schema>> globalPolymorphicTypeSchemas = new HashMap();
    private final Map<String, Map<String, Set<Schema>>> fieldPolymorphicTypeSchemas = new HashMap();
    private boolean allowNullFields;

    public AvroSchemaGenerator(boolean allowNullFields, boolean useCustomCoders, boolean defaultsGenerated) {
        if (allowNullFields) {
            this.reflectData = new ReflectDataNullableFields(this);
        } else {
            this.reflectData = ReflectData.get();
        }
        this.allowNullFields = allowNullFields;
        this.reflectData.setCustomCoders(useCustomCoders);
        this.reflectData.setDefaultsGenerated(defaultsGenerated);
    }

    public static String unionTypesToString(Schema schema) {
        return unionTypesToString(schema.getTypes());
    }

    public static String unionTypesToString(Collection<Schema> types) {
        List<String> typeNames = new ArrayList();
        for (Schema type : types) {
            typeNames.add(type.getName());
        }
        return String.join(", ", typeNames);
    }

    // Register polymorphic types
    public void declarePolymorphicType(String fieldSymbol, Type... types) {
        // Declare a polymorphic type
        recordCache.clear();
        for (Type type : types) {
            Schema subtypeSchema = reflectData.getSchema(type);
            Class superType = ((Class) type).getSuperclass();
            if (Objects.isNull(fieldSymbol)) {
                // Global assigned polymorphic types
                while (!superType.equals(Object.class)) {
                    String superTypeName = superType.getCanonicalName();
                    Set<Schema> subTypes = globalPolymorphicTypeSchemas.get(superTypeName);
                    if (Objects.isNull(subTypes)) {
                        subTypes = new HashSet();
                        globalPolymorphicTypeSchemas.put(superTypeName, subTypes);
                    }
                    subTypes.add(subtypeSchema);
                    superType = superType.getSuperclass();
                }
            } else {
                // Field assigned polymorphic types
                Map<String, Set<Schema>> fieldTypes = fieldPolymorphicTypeSchemas.get(fieldSymbol);
                if (Objects.isNull(fieldTypes)) {
                    fieldTypes = new HashMap();
                    fieldPolymorphicTypeSchemas.put(fieldSymbol, fieldTypes);
                }
                // add potential interface types
                Class<?>[] interfaceClasses = ((Class<?>) type).getInterfaces();
                HashSet typeInterfaceSet = new HashSet();
                typeInterfaceSet.add(subtypeSchema);
                for (Class interfaceClass : interfaceClasses) {
                    fieldTypes.put(interfaceClass.getCanonicalName(), typeInterfaceSet);
                }
                // Parse including root
                while (Objects.nonNull(superType)) {
                    String superTypeName = superType.getCanonicalName();
                    Set<Schema> subFieldTypes = fieldTypes.get(superTypeName);
                    if (Objects.isNull(subFieldTypes)) {
                        subFieldTypes = new HashSet();
                        fieldTypes.put(superTypeName, subFieldTypes);
                    }
                    subFieldTypes.add(subtypeSchema);
                    Class<?>[] superInterfaceClasses = ((Class<?>) superType).getInterfaces();
                    for (Class superInterfaceClass : superInterfaceClasses) {
                        Set<Schema> superTypeInterfaceSet = fieldTypes.get(superInterfaceClass.getCanonicalName());
                        if (Objects.isNull(superTypeInterfaceSet)) {
                            superTypeInterfaceSet = new HashSet();
                            fieldTypes.put(superInterfaceClass.getCanonicalName(), superTypeInterfaceSet);
                        }
                        superTypeInterfaceSet.add(subtypeSchema);
                    }
                    superType = superType.getSuperclass();
                }

            }
        }
    }

    // Create a schema for type
    public Schema generateSchema(Class clazz) {
        Schema schema = reflectData.getSchema(clazz);
        return globalPolymorphicTypeSchemas.isEmpty() ? schema : polymorphizeSchema(schema);
    }

    // Create a protocol for interface
    public Protocol generateProtocol(Class clazz) {
        Protocol protocol = reflectData.getProtocol(clazz);
        return globalPolymorphicTypeSchemas.isEmpty() ? protocol : polymorphizeProtocol(protocol);
    }

    public Schema polymorphizeSchema(Schema rootSchema) {
        AvroSchemaIterator avroSchemaIterator = new AvroSchemaIterator(rootSchema);
        for (Schema schema : avroSchemaIterator) {
            if (!Objects.equals(schema.getType(), Schema.Type.RECORD)) {
                continue;
            }
            if (recordCache.isFieldsSet(schema)) {
                continue;
            }
            // Compute a new record for schema
            computeNewRecordSchema(schema);
        }

        String emptySchemaName;
        while (Objects.nonNull(emptySchemaName = recordCache.getSchemaWithoutFields())) {
            // Create remaining schemas
            Schema customSchema = customSchemas.get(emptySchemaName);
            if (Objects.nonNull(customSchema)) {
                computeNewRecordSchema(customSchema);
                continue;
            }
            Type polymorphicType;
            try {
                // In case of inner classes look for uppercase class namings and replace dot notation from schema
                Matcher matcher = uppercaseClassNamePattern.matcher(emptySchemaName);
                boolean firstDotSkipped = false;
                StringBuilder stringBuilder = null;
                while (matcher.find()) {
                    if (firstDotSkipped) {
                        stringBuilder.setCharAt(matcher.start(), '$');
                    } else {
                        matcher.start();
                        firstDotSkipped = true;
                        stringBuilder = new StringBuilder(emptySchemaName);
                    }
                }
                if (Objects.nonNull(stringBuilder)) {
                    emptySchemaName = stringBuilder.toString();
                }
                polymorphicType = ClassUtils.forName(emptySchemaName);
            } catch (ClassNotFoundException ex) {
                throw new SchemaGenerationException("Unable to find class for " + emptySchemaName, ex);
            }
            computeNewRecordSchema(reflectData.getSchema(polymorphicType));
        }
        switch (rootSchema.getType()) {
            case RECORD:
                return recordCache.get(rootSchema);
            case ARRAY:
            case MAP:
            case UNION:
                throw new UnsupportedOperationException("Schemas with ARRAY, MAP, or UNION-type root schemas are not supported yet");
            default:
                return rootSchema;
        }
    }

    private Schema computeNewRecordSchema(Schema schema) {
        List<Field> newFields = new ArrayList();
        // Create a new schema for each of the fields
        for (Field field : schema.getFields()) {
            Schema newFieldSchema;
            Schema oldFieldSchema = field.schema();
            String fieldSymbols = schema.getFullName() + "." + field.name();

            switch (oldFieldSchema.getType()) {
                case ARRAY:
                    Schema polymorphicElementTypesSchema = getPolymorphicTypes(oldFieldSchema.getElementType(), fieldSymbols);
                    Schema polymorphicElementTypesArraySchema = Schema.createArray(polymorphicElementTypesSchema);
                    // conserve original objects properties
                    Map<String, Object> arrayProperties = oldFieldSchema.getObjectProps();
                    for (Map.Entry<String, Object> arrayProperty : arrayProperties.entrySet()) {
                        polymorphicElementTypesArraySchema.addProp(arrayProperty.getKey(), arrayProperty.getValue());
                    }
                    newFieldSchema = getPolymorphicTypes(polymorphicElementTypesArraySchema, fieldSymbols);
                    break;
                case MAP:
                    newFieldSchema = getPolymorphicTypes(Schema.createMap(getPolymorphicTypes(oldFieldSchema.getValueType(), fieldSymbols)), fieldSymbols);
                    break;
                default:
                    newFieldSchema = getPolymorphicTypes(oldFieldSchema, fieldSymbols);
                    break;
            }
            if (newFieldSchema.getTypes().get(0).getType().equals(Schema.Type.NULL)) {
                newFields.add(new Field(field.name(), newFieldSchema, field.doc(), NULL_DEFAULT_VALUE, field.order()));
            } else {
                newFields.add(new Field(field.name(), newFieldSchema, field.doc(), field.defaultVal(), field.order()));
            }
        }
        return recordCache.set(schema, newFields);
    }

    public Schema getPolymorphicTypes(Schema schema, String fieldSymbols) {
        // Get the polymorphic types for schema
        switch (schema.getType()) {
            case MAP:
            case ARRAY:
                List<Schema> newTypes = new ArrayList();
                if (allowNullFields) {
                    if (schema.getProp(LOGICAL_TYPE_PROP) != PRIMITIVE) {
                        newTypes.add(Schema.create(Schema.Type.NULL));
                    }
                }
                newTypes.add(schema);
                schema = Schema.createUnion(newTypes);
                return schema;
        }
        if (schema.getType() != Schema.Type.UNION) {
            List<Schema> newTypes = new ArrayList();
            if (allowNullFields) {
                if (schema.getProp(LOGICAL_TYPE_PROP) != PRIMITIVE) {
                    newTypes.add(Schema.create(Schema.Type.NULL));
                }
            }
            newTypes.add(schema);
            schema = Schema.createUnion(newTypes);
        }
        Deque<Schema> polyTypes = new LinkedList(schema.getTypes());
        List<Schema> finalSchemaTypes = new ArrayList();
        while (!polyTypes.isEmpty()) {
            Schema unionType = polyTypes.removeFirst();
            if (unionType.getType() == Schema.Type.RECORD) {
                unionType = recordCache.getOrCreate(unionType);
            }
            finalSchemaTypes.add(unionType);
            Collection<Schema> subTypes = null;
            if (isNamedType(unionType)) {
                String className = SpecificData.getClassName(unionType);
                // check field specific subtypes first
                Map<String, Set<Schema>> fieldPolymorphicTypeSchemaMap = fieldPolymorphicTypeSchemas.get(fieldSymbols);
                if (Objects.isNull(fieldPolymorphicTypeSchemaMap)) {
                    subTypes = globalPolymorphicTypeSchemas.get(className);
                } else {
                    subTypes = fieldPolymorphicTypeSchemaMap.get(className);
                }
            }
            if (Objects.isNull(subTypes)) {
                continue;
            }
            polyTypes.addAll(subTypes);
        }
        return Schema.createUnion(orderUnionSchemas(finalSchemaTypes));
    }

    private List<Schema> orderUnionSchemas(List<Schema> unionSchemas) {
        // Arrange the schemas in order
        Map<String, Schema> namedSchemas = new TreeMap();
        List<Schema> orderedSchemasNew = new LinkedList();
        for (Schema unionSchema : unionSchemas) {
            switch (unionSchema.getType()) {
                case BOOLEAN:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case BYTES:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case DOUBLE:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case FLOAT:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case INT:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case LONG:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case NULL:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case STRING:
                    orderedSchemasNew.add(unionSchema);
                    break;
                case ENUM:
                case FIXED:
                case RECORD:
                    namedSchemas.put(unionSchema.getFullName(), unionSchema);
                    break;
                default:
                    throw new SchemaGenerationException("Unsupported operation: Schema of type " + unionSchema.getType() + " in union");
            }
        }
        for (Schema schema : namedSchemas.values()) {
            orderedSchemasNew.add(schema);
        }
        return orderedSchemasNew;
    }

    private Protocol polymorphizeProtocol(Protocol protocol) {
        Protocol polymorphicProtocol = new Protocol(protocol.getName(), protocol.getDoc(), protocol.getNamespace());
        for (Map.Entry<String, Object> entry : protocol.getObjectProps().entrySet()) {
            polymorphicProtocol.addProp(entry.getKey(), entry.getValue());
        }
        Collection<Schema> namedTypes = new LinkedHashSet();
        for (Schema schema : protocol.getTypes()) {
            namedTypes.add(polymorphizeSchema(schema));
        }
        polymorphicProtocol.setTypes(namedTypes);
        Map<String, Protocol.Message> polymorphicMessages = polymorphicProtocol.getMessages();
        for (Protocol.Message message : protocol.getMessages().values()) {
            if (message.isOneWay()) {
                polymorphicMessages.put(message.getName(), polymorphicProtocol.createMessage(message, polymorphizeSchema(message.getRequest())));
            } else {
                Schema requestSchema = message.getRequest();
                Schema polymorphRequestSchema = polymorphizeSchema(requestSchema);
                Schema responseSchema = message.getResponse();
                Schema polymorphResponseSchema = polymorphizeSchema(responseSchema);
                polymorphicMessages.put(message.getName(), polymorphicProtocol.createMessage(message, polymorphizeSchema(message.getRequest()), polymorphizeSchema(message.getResponse()), message.getErrors()));
            }
        }
        return polymorphicProtocol;
    }

    // Return whether unionType is a named type
    private boolean isNamedType(Schema unionType) {
        try {
            unionType.getNamespace();
        } catch (AvroRuntimeException e) {
            return false;
        }
        return true;
    }

    // Declares that schemas has a custom name not on the classpath
    public void declareCustomNamedSchema(Schema schema) {
        String typeName = schema.getFullName();
        if (!(isNamedType(schema))) {
            throw new SchemaGenerationException("Schema provided for " + typeName + " is not a NamedSchema");
        }
        Schema customType = customSchemas.get(typeName);
        if (Objects.isNull(customType)) {
            customSchemas.put(typeName, schema);
            return;
        }
        if (!customType.equals(schema)) {
            throw new SchemaGenerationException("Attempted to redefine schema for existing custom type " + typeName);
        }
    }

    public void setConversions(List<Conversion<?>> conversions) {
        for (Conversion<?> conversion : conversions) {
            this.reflectData.addLogicalTypeConversion(conversion);
        }
    }

    public void addConversion(Conversion<?> conversion) {
        this.reflectData.addLogicalTypeConversion(conversion);
    }

    public ReflectData getReflectData() {
        return this.reflectData;
    }

    public boolean hasPolymorphicTypeSchemas() {
        return globalPolymorphicTypeSchemas.size() > 0;
    }

}