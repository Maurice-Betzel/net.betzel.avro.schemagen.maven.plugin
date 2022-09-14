package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class AvroConversions {

    public static class UtilDateTimestampMillis extends Conversion<Date> {

        @Override
        public Class<Date> getConvertedType() {
            return Date.class;
        }

        @Override
        public String getLogicalTypeName() {
            return AvroLogicalTypes.UTIL_DATE_TIMESTAMP_MILLIS;
        }

        @Override
        public Long toLong(Date date, Schema schema, LogicalType type) {
            return date == null ? null : date.getTime();
        }

        @Override
        public Date fromLong(Long dateLong, Schema schema, LogicalType type) {
            return dateLong == null ? null : new Date(dateLong);
        }

        @Override
        public Schema getRecommendedSchema() {
            return AvroLogicalTypes.utilDateTimestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        }
    }

    public static class ZonedDateTimestampMillis extends Conversion<ZonedDateTime> {

        private static final DateTimeFormatter DATE_TIME_FORMATTER = DateTimeFormatter.ISO_ZONED_DATE_TIME;

        @Override
        public Class<ZonedDateTime> getConvertedType() {
            return ZonedDateTime.class;
        }

        @Override
        public String getLogicalTypeName() {
            return AvroLogicalTypes.ZONED_DATE_TIMESTAMP_MILLIS;
        }

        @Override
        public CharSequence toCharSequence(ZonedDateTime zonedDateTime, Schema schema, LogicalType type) {
            return zonedDateTime == null ? null : zonedDateTime.format(DATE_TIME_FORMATTER);
        }

        @Override
        public ZonedDateTime fromCharSequence(CharSequence zonedDateTimeString, Schema schema, LogicalType type) {
            return zonedDateTimeString == null ? null : ZonedDateTime.parse(zonedDateTimeString, DATE_TIME_FORMATTER);
        }

        @Override
        public Schema getRecommendedSchema() {
            return AvroLogicalTypes.zonedDateTimestampMillis().addToSchema(Schema.create(Schema.Type.STRING));
        }
    }

    public static class SerializableMarkerInterface extends Conversion<Serializable> {

        // fallback to java.io.Serializable

        @Override
        public Class<Serializable> getConvertedType() {
            return Serializable.class;
        }

        @Override
        public String getLogicalTypeName() {
            return AvroLogicalTypes.SERIALIZABLE_MARKER_INTERFACE;
        }

        @Override
        public CharSequence toCharSequence(Serializable serializable, Schema schema, LogicalType type) {
            return "Serializable";
        }

        @Override
        public Serializable fromCharSequence(CharSequence zonedDateTimeString, Schema schema, LogicalType type) {
            return new IOException("Serializable");
        }

        @Override
        public ByteBuffer toBytes(Serializable serializable, Schema schema, LogicalType type) {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            try {
                ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
                objectOutputStream.writeObject(serializable);
                objectOutputStream.close();
            } catch (Exception e) {
                byte[] bytes = {(byte) 0};
                return ByteBuffer.wrap(bytes);
            }
            return ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
        }

        @Override
        public Serializable fromBytes(ByteBuffer byteBuffer, Schema schema, LogicalType type) {
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteBuffer.array());
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
                return (Serializable) objectInputStream.readObject();
            } catch (Exception e) {
                return null;
            }
        }

        @Override
        public Schema getRecommendedSchema() {
            return AvroLogicalTypes.serializableMarkerInterface().addToSchema(Schema.create(Schema.Type.BYTES));
        }
    }

}