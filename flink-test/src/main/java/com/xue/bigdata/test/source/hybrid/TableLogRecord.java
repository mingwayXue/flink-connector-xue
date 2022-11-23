package com.xue.bigdata.test.source.hybrid;

import com.alibaba.fastjson.JSONObject;

import java.io.Serializable;

public class TableLogRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    private JSONObject before;

    private JSONObject after;

    private JSONObject source;

    private String op;

    public TableLogRecord(JDBCSplit jdbcSplit, JSONObject jsonObject) {
        this.op = "r";
        this.after = jsonObject;
        JSONObject jObj = new JSONObject();
        jObj.put("db", jdbcSplit.getDb());
        jObj.put("table", jdbcSplit.getTable());
        this.source = jObj;
    }

    public JSONObject getBefore() {
        return before;
    }

    public void setBefore(JSONObject before) {
        this.before = before;
    }

    public JSONObject getAfter() {
        return after;
    }

    public void setAfter(JSONObject after) {
        this.after = after;
    }

    public JSONObject getSource() {
        return source;
    }

    public void setSource(JSONObject source) {
        this.source = source;
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }
}
