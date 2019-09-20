package com.moumou.bootmybatisdemo.dataAlignment.model;

public class ColumnDescInfo extends SourceField {
    private String incCdt;

    public ColumnDescInfo(String incCdt) {
        super();
        this.incCdt = incCdt;
    }

    public ColumnDescInfo() {
    }

    public String getIncCdt() {
        return incCdt;
    }

    public void setIncCdt(String incCdt) {
        this.incCdt = incCdt;
    }
}
