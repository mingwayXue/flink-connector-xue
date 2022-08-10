package com.xue.bigdata.test.cep;

/**
 * @author: mingway
 * @date: 2022/1/27 5:03 下午
 */
public class AlertEvent {
    private String id;
    private String message;
    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getMessage() {
        return message;
    }
    public void setMessage(String message) {
        this.message = message;
    }
    public AlertEvent(String id, String message) {
        this.id = id;
        this.message = message;
    }
    @Override
    public String toString() {
        return "AlertEvent{" +
                "id='" + id + '\'' +
                ", message='" + message + '\'' +
                '}';
    }
}
