package com.xue.bigdata.test.cdc;

/**
 * @author: mingway
 * @date: 2021/12/10 10:05 上午
 */
public class SubscribeRule {

    private String op;

    private Integer id;

    private String reportId;

    private Integer opType;

    private Integer shopId;

    private String userId;

    public SubscribeRule() {
    }

    public String getOp() {
        return op;
    }

    public void setOp(String op) {
        this.op = op;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getReportId() {
        return reportId;
    }

    public void setReportId(String reportId) {
        this.reportId = reportId;
    }

    public Integer getOpType() {
        return opType;
    }

    public void setOpType(Integer opType) {
        this.opType = opType;
    }

    public Integer getShopId() {
        return shopId;
    }

    public void setShopId(Integer shopId) {
        this.shopId = shopId;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    @Override
    public String toString() {
        return "SubscribeRule{" +
                "op='" + op + '\'' +
                ", id=" + id +
                ", reportId='" + reportId + '\'' +
                ", opType=" + opType +
                ", shopId=" + shopId +
                ", userId='" + userId + '\'' +
                '}';
    }
}
