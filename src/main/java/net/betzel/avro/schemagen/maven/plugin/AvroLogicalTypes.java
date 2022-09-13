package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class AvroLogicalTypes {

    public static final String UTIL_DATE_TIMESTAMP_MILLIS = "util-date-timestamp-millis";
    public static final String ZONED_DATE_TIMESTAMP_MILLIS = "zoned-date-timestamp-millis";
    public static final String SERIALIZABLE_MARKER_INTERFACE = "serializable-marker-interface";

    private static final AvroLogicalTypes.UtilDateTimestampMillis UTIL_DATE_TIMESTAMP_MILLIS_TYPE = new AvroLogicalTypes.UtilDateTimestampMillis();
    private static final AvroLogicalTypes.ZonedDateTimestampMillis ZONED_DATE_TIMESTAMP_MILLIS_TYPE = new AvroLogicalTypes.ZonedDateTimestampMillis();
    private static final AvroLogicalTypes.SerializableMarkerInterface SERIALIZABLE_MARKER_INTERFACE_TYPE = new AvroLogicalTypes.SerializableMarkerInterface();

    public static AvroLogicalTypes.UtilDateTimestampMillis utilDateTimestampMillis() {
        return UTIL_DATE_TIMESTAMP_MILLIS_TYPE;
    }
    public static AvroLogicalTypes.ZonedDateTimestampMillis zonedDateTimestampMillis() {
        return ZONED_DATE_TIMESTAMP_MILLIS_TYPE;
    }
    public static AvroLogicalTypes.SerializableMarkerInterface serializableMarkerInterface() {
        return SERIALIZABLE_MARKER_INTERFACE_TYPE;
    }

    public static class UtilDateTimestampMillis extends LogicalType {

        private UtilDateTimestampMillis() {
            super(UTIL_DATE_TIMESTAMP_MILLIS);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.LONG) {
                throw new IllegalArgumentException("Util date timestamp (millis) can only be used with an underlying long type");
            }
        }
    }

    public static class ZonedDateTimestampMillis extends LogicalType {

        private ZonedDateTimestampMillis() {
            super(ZONED_DATE_TIMESTAMP_MILLIS);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("Zoned date timestamp (millis) can only be used with an underlying string type");
            }
        }
    }

    public static class SerializableMarkerInterface extends LogicalType {

        private SerializableMarkerInterface() {
            super(SERIALIZABLE_MARKER_INTERFACE);
        }

        @Override
        public void validate(Schema schema) {
            super.validate(schema);
            if (schema.getType() != Schema.Type.STRING) {
                throw new IllegalArgumentException("Serializable marker interface java.io.Serializable can only be used with an underlying string type");
            }
        }
    }

}