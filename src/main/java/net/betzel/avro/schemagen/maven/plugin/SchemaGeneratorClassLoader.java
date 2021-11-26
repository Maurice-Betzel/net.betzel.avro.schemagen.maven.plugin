package net.betzel.avro.schemagen.maven.plugin;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SchemaGeneratorClassLoader extends ClassLoader {

    @Override
    public Class findClass(String name) throws ClassNotFoundException {
        byte[] bytes;
        try {
            bytes = loadClassFromFile2(name);
        } catch (IOException e) {
            throw new ClassNotFoundException(e.getMessage());
        }
        return defineClass("eu.abeel.platform.facade.stream.customs.ags.api.generated.AGSSswDeclaration", bytes, 0, bytes.length);
    }

    private byte[] loadClassFromFile(String fileName) throws IOException {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName.replace('.', File.separatorChar) + ".class")) {
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
            int nextValue = 0;
            while ((nextValue = inputStream.read()) != -1) {
                byteStream.write(nextValue);
            }

            byte[] buffer = byteStream.toByteArray();
            return buffer;
        }
    }

    private byte[] loadClassFromFile2(String fileName) throws IOException {
        File file = new File(fileName + ".class");
        if (file.exists() || file.isFile()) {

            try (InputStream inputStream = new FileInputStream(file)) {
                ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
                int nextValue = 0;
                while ((nextValue = inputStream.read()) != -1) {
                    byteStream.write(nextValue);
                }

                byte[] buffer = byteStream.toByteArray();
                return buffer;
            }
        } else {
            throw new IllegalArgumentException("NOT A FILE " + fileName);
        }
    }
}