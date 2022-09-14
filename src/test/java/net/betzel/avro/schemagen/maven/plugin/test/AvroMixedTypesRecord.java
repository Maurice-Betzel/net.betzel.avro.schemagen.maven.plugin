package net.betzel.avro.schemagen.maven.plugin.test;

import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Random;

public class AvroMixedTypesRecord {

    Date date;
    Throwable throwable;
    int primitiveInteger;
    Integer wrapperInteger;
    Serializable serializable;
    List<Exception> exceptions;
    HashMap<String, Date> stringDateHashMap;
    ArrayList<RuntimeException> runtimeExceptions;

    public AvroMixedTypesRecord() {
    }

    public AvroMixedTypesRecord(Random random, LocalDateTime ldt) {
        date = Date.from(ldt.toLocalDate().atStartOfDay().minusDays(2).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant());
        throwable = new IllegalArgumentException("Illegal Argument");
        wrapperInteger = random.nextInt();
        primitiveInteger = random.nextInt();
        serializable = new InterruptedException("Interrupted");
        exceptions = new ArrayList(3);
        exceptions.add(new IOException("1"));
        exceptions.add(new IOException("2"));
        exceptions.add(new NullPointerException("1"));
        stringDateHashMap = new HashMap(2);
        stringDateHashMap.put("1", Date.from(ldt.toLocalDate().atStartOfDay().minusDays(3).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant()));
        stringDateHashMap.put("2", Date.from(ldt.toLocalDate().atStartOfDay().minusDays(5).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant()));
        runtimeExceptions = new ArrayList(2);
        runtimeExceptions.add(new NullPointerException("2"));
        runtimeExceptions.add(new IllegalArgumentException("1"));
    }
}