package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Conversion;
import org.apache.avro.Protocol;
import org.apache.avro.Schema;
import org.apache.avro.util.ClassUtils;
import org.apache.maven.model.Resource;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Mojo(name = "schema-generator", defaultPhase = LifecyclePhase.PROCESS_CLASSES)
public final class SchemaGeneratorMojo extends AbstractMojo {

    public static final String AVRO_INFO = "AVRO-INF";
    @Parameter(property = "classPath", defaultValue = "${project.build.outputDirectory}", required = true)
    String classPath;
    @Parameter(property = "classFile", required = true, readonly = false)
    String classFile;
    @Parameter(property = "packageSchema", defaultValue = "true", required = false, readonly = false)
    boolean packageSchema;
    // Permits null field values. The schema generated for each field is a union of its declared type and null
    @Parameter(property = "allowNullFields", required = false, defaultValue = "false", readonly = false)
    boolean allowNullFields;
    // Beta function that speeds up decoding of objects by more than 10% and encoding by more than 30%
    @Parameter(property = "useCustomCoders", required = false, defaultValue = "false", readonly = false)
    boolean useCustomCoders;
    // Set default values for types
    @Parameter(property = "defaultsGenerated", required = false, defaultValue = "false", readonly = false)
    boolean defaultsGenerated;
    @Parameter(property = "conversionClassFiles", required = false, readonly = false)
    List<String> conversionClassFiles;
    @Parameter(property = "polymorphicClassFiles", required = false, readonly = false)
    List<String> polymorphicClassFiles;
    @Parameter(property = "fieldBoundPolymorphicClassFiles", required = false, readonly = false)
    Map<String, String> fieldBoundPolymorphicClassFiles;
    @Parameter(property = "targetSchemaPath", defaultValue = "${project.build.directory}/" + AVRO_INFO, required = false, readonly = false)
    String targetSchemaPath;
    @Parameter(defaultValue = "${project}", readonly = true)
    private MavenProject mavenProject;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Executing AVRO schema generator Maven plugin");
        File classPathDir = new File(classPath);
        File targetSchemaPathDir = new File(targetSchemaPath);
        if (!classPathDir.isDirectory()) {
            throw new SchemaGenerationException("Class path directory " + classPath + " is not a directory!");
        }
        if (!classPathDir.exists()) {
            throw new SchemaGenerationException("Class path directory " + classPath + " does not exist!");
        }
        if (!targetSchemaPathDir.exists()) {
            try {
                Files.createDirectories(targetSchemaPathDir.toPath());
            } catch (IOException e) {
                getLog().error(e);
                throw new SchemaGenerationException("Cannot create directory " + targetSchemaPathDir + "!");
            }
        }
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        getLog().debug("Context class loader hierarchy: " + ClassLoaderUtils.showClassLoaderHierarchy(contextClassLoader));
        List<Conversion<?>> conversions = new ArrayList();
        try {
            for (String conversionClassName : conversionClassFiles) {
                getLog().info("Adding AVRO conversion class " + conversionClassName);
                Class<?> conversionClazz = ClassUtils.forName(conversionClassName);
                Object conversionObject = conversionClazz.newInstance();
                if (conversionObject instanceof Conversion) {
                    conversions.add((Conversion<?>) conversionObject);
                    getLog().debug("Added AVRO conversion class " + conversionClazz.getCanonicalName());
                } else {
                    getLog().warn("Skipping non AVRO conversion class " + conversionClazz.getCanonicalName());
                }
            }
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            getLog().error(e);
            throw new MojoExecutionException(e.getMessage(), e.getCause());
        }
        try (FileClassLoader fileClassLoader = new FileClassLoader(classPathDir, contextClassLoader)) {
            Class clazz = fileClassLoader.loadClass(classFile);
            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator(allowNullFields, useCustomCoders, defaultsGenerated);
            schemaGenerator.setConversions(conversions);
            if (clazz.isInterface()) {
                getLog().info("Generating AVRO protocol for class " + clazz.getCanonicalName());
                Protocol protocol = schemaGenerator.generateProtocol(clazz);
                getLog().debug("Schema : " + protocol.toString(true));
                Path protocolPath = Paths.get(targetSchemaPathDir.getPath(), clazz.getName() + ".avpr");
                getLog().info("Writing AVRO protocol to " + protocolPath);
                Files.write(protocolPath, protocol.toString(true).getBytes(StandardCharsets.UTF_8));
            } else {
                getLog().info("Generating AVRO schema for class " + clazz.getCanonicalName());
                if (Objects.nonNull(polymorphicClassFiles) && !polymorphicClassFiles.isEmpty()) {
                    Type[] types = new Type[polymorphicClassFiles.size()];
                    for (int i = 0; i < polymorphicClassFiles.size(); i++) {
                        String polymorphicClassFile = polymorphicClassFiles.get(i);
                        Class polymorphicClazz = fileClassLoader.loadClass(polymorphicClassFile);
                        getLog().info("Adding polymorphic class " + polymorphicClazz.getCanonicalName());
                        types[i] = polymorphicClazz;
                    }
                    schemaGenerator.declarePolymorphicType(null, types);
                }
                if (Objects.nonNull(fieldBoundPolymorphicClassFiles)) {
                    for (Map.Entry<String, String> entry : fieldBoundPolymorphicClassFiles.entrySet()) {
                        String key = entry.getKey();
                        String fieldString = key.substring(key.lastIndexOf(".") + 1, key.length());
                        Field matchedField = ReflectionUtils.getFieldByNameIncludingSuperclasses(fieldString, clazz);
                        if (Objects.isNull(matchedField)) {
                            List<Field> fields = ReflectionUtils.getFieldsIncludingSuperclasses(clazz);
                            for (Field field : fields) {
                                String classField = field.getDeclaringClass().getName() + "." + field.getName();
                                if (entry.getKey().endsWith(classField)) {
                                    matchedField = field;
                                    break;
                                }
                            }
                        }
                        if (Objects.isNull(matchedField)) {
                            throw new ClassNotFoundException("No class found containing field " + fieldString);
                        }
                        String[] typesList = entry.getValue().trim().split("\\s*,+\\s*,*\\s*");
                        Type[] types = new Type[typesList.length];
                        for (int i = 0; i < typesList.length; i++) {
                            String polymorphicClassFile = typesList[i];
                            Class polymorphicClazz = fileClassLoader.loadClass(polymorphicClassFile);
                            getLog().info("Adding polymorphic class " + polymorphicClazz.getCanonicalName() + " for field " + fieldString);
                            types[i] = polymorphicClazz;
                        }
                        // always set field on base class for Avro schema
                        schemaGenerator.declarePolymorphicType(clazz.getCanonicalName() + "." + matchedField.getName(), types);
                    }
                }
                Schema schema = schemaGenerator.generateSchema(clazz);
                getLog().debug("Schema : " + schema.toString(true));
                Path schemaPath = Paths.get(targetSchemaPathDir.getPath(), clazz.getName() + ".avsc");
                getLog().info("Writing AVRO schema to " + schemaPath);
                Files.write(schemaPath, schema.toString(true).getBytes(StandardCharsets.UTF_8));
                if (packageSchema) {
                    List<Resource> resources = mavenProject.getResources();
                    Resource resource = new Resource();
                    resource.setTargetPath(AVRO_INFO);
                    resource.setDirectory(targetSchemaPathDir.getPath());
                    resources.add(resource);
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            getLog().error(e);
            throw new MojoExecutionException(e.getMessage(), e.getCause());
        }
    }

}