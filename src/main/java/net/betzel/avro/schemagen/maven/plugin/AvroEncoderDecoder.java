package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Objects;

public class AvroEncoderDecoder<T> {

    // Encoder and Decoder are not thread-safe, DatumReader and DatumWriter are thread-save

    private final Schema schema;
    private final ReflectData reflectData;

    public AvroEncoderDecoder(Schema schema) {
        this(schema, null);
    }

    public AvroEncoderDecoder(Schema schema, ReflectData reflectData) {
        this.schema = schema;
        this.reflectData = Objects.isNull(reflectData) ? ReflectData.get() : reflectData;
    }

    public void encodeAvro(OutputStream outputStream, T type) throws IOException {
        DatumWriter<T> datumWriter = reflectData.createDatumWriter(schema);
        BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(outputStream, null);
        datumWriter.write(type, binaryEncoder);
        binaryEncoder.flush();
        outputStream.flush();
    }

    public T decodeAvro(InputStream inputStream) throws IOException {
        DatumReader<T> datumReader = reflectData.createDatumReader(schema);
        BinaryDecoder binaryDecoder = DecoderFactory.get().binaryDecoder(inputStream, null);
        return datumReader.read(null, binaryDecoder);
    }

    public Schema getSchema() {
        return schema;
    }

}