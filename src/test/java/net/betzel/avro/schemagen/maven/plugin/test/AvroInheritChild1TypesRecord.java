package net.betzel.avro.schemagen.maven.plugin.test;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class AvroInheritChild1TypesRecord extends AvroInheritParentTypesRecord {

    private LocalDate localDate;

    public AvroInheritChild1TypesRecord() {
    }

    public AvroInheritChild1TypesRecord(LocalDateTime localDateTime) {
        super(localDateTime);
        this.localDate = localDateTime.toLocalDate();
    }

}