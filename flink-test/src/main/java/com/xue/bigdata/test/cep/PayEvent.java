package com.xue.bigdata.test.cep;

/**
 * @author: mingway
 * @date: 2022/1/27 4:39 下午
 */
public class PayEvent {
    private Long userId;
    private String action;
    private Long timeStamp;

    public PayEvent(Long userId, String action, Long timeStamp) {
        this.userId = userId;
        this.action = action;
        this.timeStamp = timeStamp;
    }

    public Long getUserId() {
        return userId;
    }
    public void setUserId(Long userId) {
        this.userId = userId;
    }
    public String getAction() {
        return action;
    }
    public void setAction(String action) {
        this.action = action;
    }
    public Long getTimeStamp() {
        return timeStamp;
    }
    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "PayEvent{" +
                "userId=" + userId +
                ", action='" + action + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
