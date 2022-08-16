package com.xue.bigdata.test.util;

/**
 * Debezium 消息对象
 * @author: mingway
 * @date: 2021/11/30 8:45 下午
 */
public class DebeziumRecord {

    private String rowKey;

    private String rowKeyVal;

    private String topic;

    private String msg;

    public DebeziumRecord() {}

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getRowKeyVal() {
        return rowKeyVal;
    }

    public void setRowKeyVal(String rowKeyVal) {
        this.rowKeyVal = rowKeyVal;
    }

    @Override
    public String toString() {
        return "DebeziumRecord{" +
                "\"rowKey\":\"" + rowKey + "\"" +
                ", \"rowKeyVal\":\"" + rowKeyVal + "\"" +
                ", \"topic\":\"" + topic + "\"" +
                ", \"msg\":\"" + msg + "\"" +
                "}";
    }
}
