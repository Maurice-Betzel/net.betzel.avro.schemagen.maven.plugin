package net.betzel.avro.schemagen.maven.plugin;

import org.apache.avro.Conversion;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
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

//        private Instant;
//        private ZoneId;

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

}