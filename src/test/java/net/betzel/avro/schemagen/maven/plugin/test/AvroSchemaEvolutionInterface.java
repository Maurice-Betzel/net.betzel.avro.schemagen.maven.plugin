package net.betzel.avro.schemagen.maven.plugin.test;

import java.time.LocalDateTime;
import java.util.Date;

public interface AvroSchemaEvolutionInterface {

    Date getDate();

    void setDate(LocalDateTime localDateTime);

}