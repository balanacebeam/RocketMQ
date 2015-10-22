package com.alibaba.rocketmq.store.transaction.jdbc;

import com.alibaba.rocketmq.common.config.Config;

/**
 * Created by diwayou on 2015/10/21.
 */
public class ShardingJdbcTransactionStore extends AbstractJdbcTransactionStore {

    public ShardingJdbcTransactionStore(Config config) {
        super(config);
    }

    @Override
    public boolean start() {
        return false;
    }

    @Override
    public boolean shutdown() {
        return false;
    }
}
