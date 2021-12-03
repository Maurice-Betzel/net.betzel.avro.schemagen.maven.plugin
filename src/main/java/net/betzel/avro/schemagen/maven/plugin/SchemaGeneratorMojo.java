package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Mojo(name = "schema-generator", defaultPhase = LifecyclePhase.PROCESS_CLASSES)
public class SchemaGeneratorMojo extends AbstractMojo {

    @Parameter(property = "classPath", defaultValue = "${project.build.outputDirectory}", required = true)
    String classPath;

    @Parameter(property = "classFile", required = true, readonly = false)
    String classFile;

    @Parameter(property = "polymorphicClassFiles", required = false, readonly = false)
    List<String> polymorphicClassFiles;

    @Parameter(property = "targetSchemaPath", defaultValue = "${project.build.outputDirectory}/META-INF/avro/schemas", required = false, readonly = false)
    String targetSchemaPath;

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
                throw new SchemaGenerationException("Cannot create directory " + targetSchemaPathDir + "!");
            }
        }
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try (FileClassLoader fileClassLoader = new FileClassLoader(classPathDir, contextClassLoader)) {
            Class clazz = fileClassLoader.loadClass(classFile);
            getLog().info("Generating AVRO schema for class " + clazz.getCanonicalName());
            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
            if (Objects.nonNull(polymorphicClassFiles) && !polymorphicClassFiles.isEmpty()) {
                Type[] types = new Type[polymorphicClassFiles.size()];
                for (int i = 0; i < polymorphicClassFiles.size(); i++) {
                    String polymorphicClassFile = polymorphicClassFiles.get(i);
                    Class polymorphicClazz = fileClassLoader.loadClass(polymorphicClassFile);
                    types[i] = polymorphicClazz;
                }
                schemaGenerator.declarePolymorphicType(types);
            }
            Schema schema = schemaGenerator.generateSchema(clazz);
            getLog().debug("Schema : " + schema.toString(true));
            Path schemaPath = Paths.get(targetSchemaPathDir.getPath(), clazz.getName() + ".avsc");
            Files.write(schemaPath, schema.toString(true).getBytes(StandardCharsets.UTF_8));
        } catch (IOException | ClassNotFoundException e) {
            throw new MojoExecutionException(e.getMessage(), e.getCause());
        }
    }

}