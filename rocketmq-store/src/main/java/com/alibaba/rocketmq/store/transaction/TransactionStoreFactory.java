package com.alibaba.rocketmq.store.transaction;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import com.alibaba.rocketmq.store.transaction.jdbc.ShardingJdbcTransactionStore;
import com.alibaba.rocketmq.store.transaction.jdbc.SimpleJdbcTransactionStore;

/**
 * Created by diwayou on 2015/10/21.
 */
public class TransactionStoreFactory {

    public static TransactionStore getTransactionStore(Config config) {
        TransactionConfig.StoreType storeType = config.transactionConfig.storeType;

        switch (storeType) {
            case jdbc:
                return new SimpleJdbcTransactionStore(config);
            case sharding_jdbc:
                return new ShardingJdbcTransactionStore(config);
            default:
                throw new RuntimeException("No transaction implementation for " + storeType);
        }
    }
}
