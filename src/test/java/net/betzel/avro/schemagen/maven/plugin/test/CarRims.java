package net.betzel.avro.schemagen.maven.plugin.test;

public class CarRims {

    private float inches;
    private boolean hasExtraWide = false;

    public CarRims() {
    }

    public float getInches() {
        return inches;
    }

    public void setInches(float inches) {
        this.inches = inches;
    }

    public boolean isHasExtraWide() {
        return hasExtraWide;
    }

    public void setHasExtraWide(boolean hasExtraWide) {
        this.hasExtraWide = hasExtraWide;
    }

}