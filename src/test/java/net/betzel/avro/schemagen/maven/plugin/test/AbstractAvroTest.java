package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroEncoderDecoder;
import org.apache.avro.Schema;
import org.apache.avro.reflect.ReflectData;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public abstract class AbstractAvroTest {

    static byte[] encode(ReflectData reflectData, Schema schema, Object object) throws IOException {
        AvroEncoderDecoder serializer = new AvroEncoderDecoder(schema, reflectData);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        serializer.encodeAvro(byteArrayOutputStream, object);
        return byteArrayOutputStream.toByteArray();
    }

    static <T> T decode(ReflectData reflectData, Schema schema, byte[] bytes) throws IOException {
        AvroEncoderDecoder<T> serializer = new AvroEncoderDecoder(schema, reflectData);
        return serializer.decodeAvro(new ByteArrayInputStream(bytes));
    }


}
