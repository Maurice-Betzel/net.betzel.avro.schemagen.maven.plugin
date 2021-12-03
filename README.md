# avro schemagenerator maven plugin

This maven plugin uses Avro's ReflectData class to generate a schema from a class on the classpath.\
ReflectData does not natively support adding inherited types to an Avro schema.\
This is resolved by providing an interface to automatically modify the schema to accommodate inherited types.\
All that is required is a one-line declaration for each inherited type to replace all instances of the base type in the schema with a union for that type and all the subtypes which have been declared.\
This enables developers to quickly create an Avro schema from an existing Java class hierarchy, even when the Java class hierarchy uses polymorphic types.