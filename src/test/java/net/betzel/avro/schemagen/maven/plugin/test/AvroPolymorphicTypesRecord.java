package net.betzel.avro.schemagen.maven.plugin.test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class AvroPolymorphicTypesRecord {

    public Throwable throwable;
    public Exception exception;
    public RuntimeException runtimeException;
    public List<Throwable> throwables;
    public HashSet<Exception> exceptions;
    public Map<String, Exception> exceptionMap;
    //public List<Serializable> serializables;

}