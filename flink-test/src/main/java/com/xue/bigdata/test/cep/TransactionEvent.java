package com.xue.bigdata.test.cep;

/**
 * @author: mingway
 * @date: 2022/1/27 4:39 下午
 */
public class TransactionEvent {
    private String accout;
    private Double amount;
    private Long timeStamp;

    public TransactionEvent(String accout, Double amount, Long timeStamp) {
        this.accout = accout;
        this.amount = amount;
        this.timeStamp = timeStamp;
    }

    public String getAccout() {
        return accout;
    }
    public void setAccout(String accout) {
        this.accout = accout;
    }
    public Double getAmount() {
        return amount;
    }
    public void setAmount(Double amount) {
        this.amount = amount;
    }
    public Long getTimeStamp() {
        return timeStamp;
    }
    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
