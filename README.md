# avro schemagenerator maven plugin

This maven plugin uses Avro's ReflectData class to generate a schema from a class on the classpath.\
ReflectData does not natively support adding inherited types to an Avro schema.\
This is resolved by providing an interface to automatically modify the schema to accommodate inherited types.\
All that is required is a one-line declaration for each inherited type to replace all instances of the base type in the schema with a union for that type and all the subtypes which have been declared.\
This enables developers to quickly create an Avro schema from an existing Java class hierarchy, even when the Java class hierarchy uses polymorphic types.

## License

Licensed under the Apache License, Version 2.0 (the "License").\
you may not use these files except in compliance with the License.

You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,\
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\
See the License for the specific language governing permissions and limitations under the License.