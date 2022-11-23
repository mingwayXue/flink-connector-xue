package com.xue.bigdata.test.source.hybrid;

import java.io.Serializable;

public class TableSelect implements Serializable {

    private static final long serialVersionUID = 1L;

    private String db;

    private String table;

    private String selectSql;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getSelectSql() {
        return selectSql;
    }

    public void setSelectSql(String selectSql) {
        this.selectSql = selectSql;
    }
}
