package com.alibaba.rocketmq.store.transaction;

import java.util.Date;
import java.util.List;
import java.util.Map;


/**
 * 事务存储接口，主要为分布式事务消息存储服务
 */
public interface TransactionStore {

    boolean start();

    boolean shutdown();

    boolean put(final List<TransactionRecord> transactionRecordList);

    void remove(final List<TransactionRecord> transactionRecordList);

    Date getStoreTime(String producerGroup);

    List<TransactionRecord> traverse(Map<String, Object> context, final TransactionRecord transactionRecord, final int pageSize);
}
