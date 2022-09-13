package net.betzel.avro.schemagen.maven.plugin.test;

import java.util.List;

public class AvroPolymorphicTypesRecord {

    public Throwable throwable;
    public Exception exception;
    public RuntimeException runtimeException;
    public List<Throwable> throwables;
    public List<Exception> exceptions;
    //public List<Serializable> serializables;

}