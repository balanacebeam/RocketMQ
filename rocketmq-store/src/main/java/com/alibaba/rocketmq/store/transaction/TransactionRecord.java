package com.alibaba.rocketmq.store.transaction;

import java.util.Date;

public class TransactionRecord {

    // 配置文件中指定的，不要轻易修改
    private String brokerName;

    // Commit Log Offset
    private long offset;

    private String producerGroup;

    private Date gmtCreate;

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getProducerGroup() {
        return producerGroup;
    }

    public void setProducerGroup(String producerGroup) {
        this.producerGroup = producerGroup;
    }

    public Date getGmtCreate() {
        return gmtCreate;
    }

    public void setGmtCreate(Date gmtCreate) {
        this.gmtCreate = gmtCreate;
    }
}
