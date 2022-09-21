package net.betzel.avro.schemagen.maven.plugin.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.HashMap;

public abstract class AvroInheritParentTypesRecord {

    private Date date;
    private HashMap<String, Date> stringDateHashMap = new HashMap(2);

    public AvroInheritParentTypesRecord() {
    }

    public AvroInheritParentTypesRecord(LocalDateTime localDateTime) {
        date = Date.from(localDateTime.toLocalDate().atStartOfDay().minusDays(2).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant());
        stringDateHashMap.put("1", Date.from(localDateTime.toLocalDate().atStartOfDay().minusDays(3).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant()));
        stringDateHashMap.put("2", Date.from(localDateTime.toLocalDate().atStartOfDay().minusDays(5).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant()));
    }

}