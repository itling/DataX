package com.alibaba.datax.plugin.writer.tdengine30writer;

public class TableMeta {
    public TableType tableType;
    public String tbname;
    public int columns;
    public int tags;
    public int tables;
    public String stable_name;

    @Override
    public String toString() {
        return "TableMeta{" +
                "tableType=" + tableType +
                ", tbname='" + tbname + '\'' +
                ", columns=" + columns +
                ", tags=" + tags +
                ", tables=" + tables +
                ", stable_name='" + stable_name + '\'' +
                '}';
    }
}
