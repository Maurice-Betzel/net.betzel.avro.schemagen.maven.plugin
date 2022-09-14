package net.betzel.avro.schemagen.maven.plugin.test;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class AvroMixedTypesRecord {

    Date date;
    Throwable throwable;
    Integer wrapperInteger;
    int primitiveInteger;
    List<Exception> exceptions;

    public AvroMixedTypesRecord() {
    }

    public AvroMixedTypesRecord(Random random, LocalDateTime ldt) {
        date = Date.from(ldt.toLocalDate().atStartOfDay().minusDays(2).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant());
        throwable = new IllegalArgumentException("Illegal Argument");
        wrapperInteger = random.nextInt();
        primitiveInteger = random.nextInt();
        exceptions = new ArrayList(3);
        exceptions.add(new IOException("1"));
        exceptions.add(new IOException("2"));
        exceptions.add(new NullPointerException("Null"));
    }
}