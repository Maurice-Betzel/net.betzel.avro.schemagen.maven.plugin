package net.betzel.avro.schemagen.maven.plugin.test;

import net.betzel.avro.schemagen.maven.plugin.AvroConversions;
import net.betzel.avro.schemagen.maven.plugin.AvroEncoderDecoder;
import org.apache.avro.Conversion;
import org.apache.avro.Conversions;
import org.apache.avro.Schema;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.reflect.ReflectData;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public abstract class AbstractAvroTest {

    final Random random = new Random(13L);
    final LocalDateTime localDateTime = LocalDateTime.now();
    final ZonedDateTime zonedDateTime = ZonedDateTime.now();
    final Javers javers = JaversBuilder.javers().build();
    final List<Conversion<?>> conversions = new ArrayList();

    {
        conversions.add(new Conversions.UUIDConversion());
        conversions.add(new TimeConversions.DateConversion());
        conversions.add(new TimeConversions.TimeMillisConversion());
        conversions.add(new AvroConversions.UtilDateTimestampMillis());
        conversions.add(new AvroConversions.ZonedDateTimestampMillis());
        conversions.add(new TimeConversions.TimestampMillisConversion());
        conversions.add(new TimeConversions.LocalTimestampMillisConversion());
    }

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