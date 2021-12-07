package net.betzel.avro.schemagen.maven.plugin.test;


public interface ProtocolTestInterface {

    void insert(Car car);

    boolean update(Car car);

    Car getCarByColor(String color);

    Cars getCars();

}