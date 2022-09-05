package net.betzel.avro.schemagen.maven.plugin.test;

import java.util.Random;

public class AvroPrimitiveTypesRecord {

    public double d1;
    public double d11;
    public float f2;
    public float f22;
    public int f3;
    public int f33;
    public long f4;
    public long f44;
    public byte f5;
    public byte f55;
    public short f6;
    public short f66;
    public boolean b1;
    public boolean b2;
    public String s1;
    public String s2;

    public int[] ints;
    public byte[] bytes;
    public double[] doubles;

    public short[][]shorts;

    public AvroPrimitiveTypesRecord() {
    }

    public AvroPrimitiveTypesRecord(Random random) {
        this.d1 = random.nextDouble();
        this.d11 = random.nextDouble();
        this.f2 = random.nextFloat();
        this.f22 = random.nextFloat();
        this.f3 = random.nextInt();
        this.f33 = random.nextInt();
        this.f4 = random.nextLong();
        this.f44 = random.nextLong();
        this.f5 = (byte) random.nextInt();
        this.f55 = (byte) random.nextInt();
        this.f6 = (short) random.nextInt();
        this.f66 = (short) random.nextInt();
        this.b1 = true;
        this.b2 = false;
        this.s1 = null;
        this.s2 = random.toString();
        this.ints = new int[]{random.nextInt(), random.nextInt(), random.nextInt()};
        this.bytes = new byte[]{(byte) random.nextInt(), (byte) random.nextInt(), (byte) random.nextInt(), (byte) random.nextInt()};
        this.doubles = new double[]{random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble(), random.nextDouble()};
        this.shorts = new short[][]{{1,101},{2, 102},{3,103},{4,104},{5,105}};
    }

}