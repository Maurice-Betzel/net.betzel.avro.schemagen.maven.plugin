package net.betzel.avro.schemagen.maven.plugin.test;

import java.time.LocalDateTime;
import java.time.LocalTime;

public final class AvroInheritChild2TypesRecord extends AvroInheritChild1TypesRecord {

    private LocalTime localTime;

    private AvroInheritChild2TypesRecord() {
    }

    public AvroInheritChild2TypesRecord(LocalDateTime localDateTime) {
        super(localDateTime);
        this.localTime = localDateTime.toLocalTime();
    }

}