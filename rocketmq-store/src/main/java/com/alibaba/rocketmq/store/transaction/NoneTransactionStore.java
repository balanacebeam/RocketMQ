package com.alibaba.rocketmq.store.transaction;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by diwayou on 2016/1/25.
 */
public class NoneTransactionStore implements TransactionStore {

    @Override
    public boolean start() {
        return true;
    }

    @Override
    public boolean shutdown() {
        return true;
    }

    @Override
    public boolean put(List<TransactionRecord> transactionRecordList) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void remove(List<TransactionRecord> transactionRecordList) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Date getStoreTime(String producerGroup) {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<TransactionRecord> traverse(Map<String, Object> context, TransactionRecord transactionRecord, int pageSize) {
        throw new UnsupportedOperationException("Not implemented");
    }
}
