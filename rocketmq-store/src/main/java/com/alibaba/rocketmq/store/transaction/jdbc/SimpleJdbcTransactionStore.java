package com.alibaba.rocketmq.store.transaction.jdbc;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;

/**
 * 每个broker单库单表的时候使用
 */
public class SimpleJdbcTransactionStore extends AbstractJdbcTransactionStore {

    public SimpleJdbcTransactionStore(Config config) {
        super(config);
    }

    @Override
    public boolean start() {
        TransactionConfig transactionConfig = config.transactionConfig;

        if (CollectionUtils.isEmpty(transactionConfig.urls)) {
            log.error("must config urls in transaction config.");
            return false;
        }

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUsername(transactionConfig.userName);
        dataSource.setPassword(transactionConfig.password);
        dataSource.setDriverClassName(transactionConfig.driverClassName);
        dataSource.setInitialSize(transactionConfig.initialSize);
        dataSource.setMaxTotal(transactionConfig.maxTotal);
        dataSource.setMaxIdle(transactionConfig.maxIdle);
        dataSource.setMinIdle(transactionConfig.minIdle);
        dataSource.setMaxWaitMillis(transactionConfig.maxWaitMillis);

        dataSource.setUrl(transactionConfig.urls.get(0));

        setDataSource(dataSource);

        return true;
    }

    @Override
    public boolean shutdown() {
        try {
            ((BasicDataSource)getDataSource()).close();
        } catch (Exception e) {
            log.warn("", e);
        }

        return true;
    }
}
