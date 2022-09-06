package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

public class AvroLogicalTypes {

    public static final String UTIL_DATE_TIMESTAMP_MILLIS = "util-date-timestamp-millis";
    public static final String ZONED_DATE_TIMESTAMP_MILLIS = "zoned-date-timestamp-millis";


    private static final AvroLogicalTypes.UtilDateTimestampMillis UTIL_DATE_TIMESTAMP_MILLIS_TYPE = new AvroLogicalTypes.UtilDateTimestampMillis();

    public static AvroLogicalTypes.UtilDateTimestampMillis utilDateTimestampMillis() {
        return UTIL_DATE_TIMESTAMP_MILLIS_TYPE;
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

    private static final AvroLogicalTypes.ZonedDateTimestampMillis ZONED_DATE_TIMESTAMP_MILLIS_TYPE = new AvroLogicalTypes.ZonedDateTimestampMillis();

    public static AvroLogicalTypes.ZonedDateTimestampMillis zonedDateTimestampMillis() {
        return ZONED_DATE_TIMESTAMP_MILLIS_TYPE;
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

}