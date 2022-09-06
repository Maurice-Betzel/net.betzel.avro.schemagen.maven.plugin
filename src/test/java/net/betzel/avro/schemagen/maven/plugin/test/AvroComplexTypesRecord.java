package net.betzel.avro.schemagen.maven.plugin.test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static java.math.BigDecimal.ROUND_HALF_UP;

public class AvroComplexTypesRecord {

    public Exception exception;
    public RuntimeException runtimeException;

    // Date and Time
    // Date is initialized with the current System time on deserialization
    public Day day;
    public Planet planet;
    public List<String> stringList;
    public HashSet<String> stringSet;
    public LinkedHashSet<Integer> integerSet;
    public Map<String, Integer> stringIntegerMap;

    // Numbers
    public Map<String, Double> stringDoubleMap;
    Date date;
    Instant instant;

    // Enum
    LocalDate localDate;
    LocalTime localTime;

    // Collections
    // List and Map are fully supported but Set is only partially supported with ReflectDataWriter.
    // You need to explicitly declare actual type of Set in class field declaration.
    // Map keys are assumed to be strings.
    LocalDateTime localDateTime;
    ZonedDateTime zonedDateTime;
    UUID uuid;
    BigDecimal bigDecimal;
    BigInteger bigInteger;

    public AvroComplexTypesRecord() {
    }

    public AvroComplexTypesRecord(Random random) {
        exception = new Exception(Exception.class.getSimpleName());
        runtimeException = new RuntimeException(RuntimeException.class.getSimpleName());

        date = Date.from(LocalDate.now().atStartOfDay().minusDays(2).atZone(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS).toInstant());
        instant = LocalDateTime.now().minusDays(2).minusHours(2).toInstant(ZoneOffset.UTC);
        localDate = LocalDate.now().minusDays(2);
        localTime = LocalTime.now().minusHours(2);
        localDateTime = LocalDateTime.now().minusDays(2).minusHours(2);
        zonedDateTime = ZonedDateTime.now().minusDays(2).minusHours(2).withZoneSameInstant(ZoneId.of(ZoneId.SHORT_IDS.get("ACT")));

        uuid = UUID.randomUUID();
        bigDecimal = new BigDecimal(random.nextFloat()).setScale(3, ROUND_HALF_UP);
        bigInteger = BigInteger.ZERO.setBit(63);


        day = Day.SUNDAY;
        planet = Planet.EARTH;

        stringSet = new HashSet(5);
        stringSet.add("A");
        stringSet.add("B");
        stringSet.add("C");
        stringSet.add("D");
        stringSet.add("E");
        integerSet = new LinkedHashSet<>(5);
        integerSet.add(5);
        integerSet.add(4);
        integerSet.add(3);
        integerSet.add(2);
        integerSet.add(1);
        stringList = new ArrayList(5);
        stringList.add("A");
        stringList.add("A");
        stringList.add("A");
        stringList.add("A");
        stringList.add("A");

        // Map keys are assumed to be strings

        stringIntegerMap = new HashMap(5);
        stringIntegerMap.put("A", random.nextInt());
        stringIntegerMap.put("B", random.nextInt());
        stringIntegerMap.put("C", random.nextInt());
        stringIntegerMap.put("D", random.nextInt());
        stringIntegerMap.put("E", random.nextInt());
        stringDoubleMap = new LinkedHashMap<>(5);
        stringDoubleMap.put("A", random.nextDouble());
        stringDoubleMap.put("A", random.nextDouble());
        stringDoubleMap.put("A", random.nextDouble());
        stringDoubleMap.put("A", random.nextDouble());
        stringDoubleMap.put("A", random.nextDouble());
    }

    public enum Day {
        SUNDAY, MONDAY, TUESDAY, WEDNESDAY, THURSDAY, FRIDAY, SATURDAY
    }

    public enum Planet {
        MERCURY(3.303e+23, 2.4397e6),
        VENUS(4.869e+24, 6.0518e6),
        EARTH(5.976e+24, 6.37814e6),
        MARS(6.421e+23, 3.3972e6),
        JUPITER(1.9e+27, 7.1492e7),
        SATURN(5.688e+26, 6.0268e7),
        URANUS(8.686e+25, 2.5559e7),
        NEPTUNE(1.024e+26, 2.4746e7);

        // universal gravitational constant  (m3 kg-1 s-2)
        public static final double G = 6.67300E-11;
        private final double mass;   // in kilograms
        private final double radius; // in meters

        Planet(double mass, double radius) {
            this.mass = mass;
            this.radius = radius;
        }

        private double mass() {
            return mass;
        }

        private double radius() {
            return radius;
        }

        double surfaceGravity() {
            return G * mass / (radius * radius);
        }

        double surfaceWeight(double otherMass) {
            return otherMass * surfaceGravity();
        }
    }

}