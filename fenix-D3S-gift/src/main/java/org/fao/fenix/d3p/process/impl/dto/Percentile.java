package org.fao.fenix.d3p.process.impl.dto;


public class Percentile {
    public int percentile;
    public double value;

    public Percentile() {
    }
    public Percentile(int percentile, double value) {
        this.percentile = percentile;
        this.value = value;
    }
}
