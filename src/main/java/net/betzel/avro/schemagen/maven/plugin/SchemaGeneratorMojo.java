package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.project.MavenProject;

import java.io.File;
import java.io.IOException;
import java.util.List;

@Mojo(name = "schema-generator", defaultPhase = LifecyclePhase.PROCESS_CLASSES)
public class SchemaGeneratorMojo extends AbstractMojo {

    @Parameter(defaultValue = "${project}", required = true, readonly = true)
    MavenProject project;

    @Parameter(defaultValue = "${project.build.outputDirectory}", required = true)
    String sourceClassPath;

    @Parameter(property = "sourceClassFile", required = true, readonly = false)
    String sourceClassFile;

    @Parameter(property = "targetClassFile", required = true, readonly = false)
    String targetClassFile;

    @Parameter(property = "targetSchemaPath", defaultValue = "${project.build.outputDirectory}/META-INF/avro/schemas", required = false, readonly = false)
    String targetSchemaPath;

    @Parameter(property = "polymorphicSourceFiles", required = false, readonly = false)
    List<String> polymorphicSourceFiles;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        getLog().info("Executing AVRO schema generator Maven plugin");
        File classPathDir = new File(sourceClassPath);
        File targetSchemaPathDir = new File(targetSchemaPath);
        if (!classPathDir.isDirectory()) {
            throw new SchemaGenerationException("Class path directory " + sourceClassPath + " is not a directory!");
        }
        if (!classPathDir.exists()) {
            throw new SchemaGenerationException("Class path directory " + sourceClassPath + " does not exist!");
        }
        if (!targetSchemaPathDir.isDirectory()) {
            throw new SchemaGenerationException("Avro schema path directory " + targetSchemaPathDir + " is not a directory!");
        }
        if (!targetSchemaPathDir.exists()) {
            throw new SchemaGenerationException("Avro schema path directory " + targetSchemaPathDir + " does not exist!");
        }


        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try (FileClassLoader fileClassLoader = new FileClassLoader(classPathDir, contextClassLoader)) {
            Class clazz = fileClassLoader.loadClass(sourceClassFile);
            getLog().info("Generating AVRO schema for class " + clazz.getCanonicalName());
            AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
            Schema schema = schemaGenerator.generateSchema(clazz);
            getLog().info("Schema : " + schema.toString(true));
        } catch (IOException | ClassNotFoundException e) {
            throw new MojoExecutionException(e.getMessage(), e.getCause());
        }


//        getLog().info("Generating AVRO schema for class " + classFile);
//        AvroSchemaGenerator schemaGenerator = new AvroSchemaGenerator();
//        if (Objects.nonNull(polymorphicSourceFiles) && !polymorphicSourceFiles.isEmpty()) {
//            Type[] types = new Type[polymorphicSourceFiles.size()];
//            for (int i = 0; i < polymorphicSourceFiles.size(); i++) {
//                String polymorphicSourceFileEntry = polymorphicSourceFiles.get(i);
//                String polymorphicSourceFilePath = sourceRoot + "/" + polymorphicSourceFileEntry + ".class";
//                File polymorphicSourceFile = new File(polymorphicSourceFilePath);
//
//                types[i] = polymorphicSourceFile.getClass();
//            }
//            schemaGenerator.declarePolymorphicType(types);
//        }
//        try {

        //File classPath = new File("C:\\Dev\\eu.abeel.platform.facade.stream.customs.ags-avro\\api\\target\\classes\\eu\\abeel\\platform\\facade\\stream\\customs\\ags\\api\\generated\\");


//            File test = new File("C:\\Dev\\eu.abeel.platform.facade.stream.customs.ags-avro\\api\\target\\classes\\eu\\abeel\\platform\\facade\\stream\\customs\\ags\\api\\generated\\");
//            URL url = test.toURI().toURL();
//            URL[] urls = new URL[]{url};
//            URLClassLoader urlClassLoader = new URLClassLoader(urls);
//
//            Class clazz2 = urlClassLoader.loadClass("eu.abeel.platform.facade.stream.customs.ags.api.generated.AGSSswDeclaration", true);


//            FileClassLoader2 fileClassLoader2 = new FileClassLoader2(urls);
//            InputStream inputStream = fileClassLoader.getResourceAsStream("C:\\Dev\\eu.abeel.platform.facade.stream.customs.ags-avro\\api\\target\\classes\\eu\\abeel\\platform\\facade\\stream\\customs\\ags\\api\\generated\\AGSSswDeclaration.class");
//            Class clazz2 = fileClassLoader2.loadClass("eu.abeel.platform.facade.stream.customs.ags.api.generated.AGSSswDeclaration");


//            try (URLClassLoader urlClassLoader = new URLClassLoader(urls)) {
        //SchemaGeneratorClassLoader classLoader = new SchemaGeneratorClassLoader();
        //Class clazz = classLoader.findClass("C:\\Dev\\eu.abeel.platform.facade.stream.customs.ags-avro\\api\\target\\classes\\eu\\abeel\\platform\\facade\\stream\\customs\\ags\\api\\generated\\AGSSswDeclaration");
//            File filePath = new File(sourceRoot);
//            URL url = filePath.toURI().toURL();
//            URL[] urls = new URL[]{url};
//
//            ClassLoader classLoader = new URLClassLoader(urls);
//            Thread.currentThread().setContextClassLoader(classLoader);
        // Load in the class; MyClass.class should be located in
        // the directory file:/c:/myclasses/com/mycompany

        //Class clazz = urlClassLoader.loadClass("eu.abeel.platform.facade.stream.customs.ags.api.generated.AGSSswDeclaration");
//            Schema schema = schemaGenerator.generateSchema(clazz);
//            getLog().info("Schema : " + schema.toString(true));
//        } catch (ClassNotFoundException | MalformedURLException e) {
//            e.printStackTrace();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            Thread.currentThread().setContextClassLoader(contextClassLoader);
//        }
    }

    private boolean isWindows() {
        String osName = System.getProperty("os.name");
        if (osName == null) {
            return false;
        }
        return osName.startsWith("Windows");
    }

}