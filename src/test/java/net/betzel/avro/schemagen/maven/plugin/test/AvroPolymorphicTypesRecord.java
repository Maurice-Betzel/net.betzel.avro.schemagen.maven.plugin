package net.betzel.avro.schemagen.maven.plugin.test;

import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class AvroPolymorphicTypesRecord {

    public Object object;
    public Throwable throwable;
    public Exception exception;
    public RuntimeException runtimeException;
    public Serializable serializable;
    public List<Throwable> throwables;
    public HashSet<Exception> exceptions;
    public List<Serializable> serializables;
    public Map<String, Exception> exceptionMap;

}