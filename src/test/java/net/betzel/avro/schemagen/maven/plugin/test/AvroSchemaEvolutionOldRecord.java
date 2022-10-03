package net.betzel.avro.schemagen.maven.plugin.test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Date;

public class AvroSchemaEvolutionOldRecord implements AvroSchemaEvolutionInterface {

    Date dateNow;

    public AvroSchemaEvolutionOldRecord() {
    }

    @Override
    public Date getDate() {
        return dateNow;
    }

    public void setDate(LocalDateTime localDateTime) {
        dateNow = Date.from(localDateTime.toLocalDate().atStartOfDay().minusDays(2).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant());
    }

}