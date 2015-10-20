package com.alibaba.rocketmq.broker.transaction;

import java.util.Date;
import java.util.List;


/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {

    public boolean put(final List<TransactionRecord> transactionRecordList);

    public void remove(final List<TransactionRecord> transactionRecordList);
    
    public Date getStoreTime();

    public List<TransactionRecord> traverse(final TransactionRecord transactionRecord, final int pageSize);
}
