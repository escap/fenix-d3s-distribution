package org.fao.fenix.d3p.process.impl.dto;


public class Percentile {
    public double percentile;
    public double value;

    public Percentile() {
    }
    public Percentile(double percentile, double value) {
        this.percentile = percentile;
        this.value = value;
    }
}
