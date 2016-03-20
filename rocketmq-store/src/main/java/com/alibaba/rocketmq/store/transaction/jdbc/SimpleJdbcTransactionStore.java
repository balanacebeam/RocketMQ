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

        BasicDataSource basicDataSource = new BasicDataSource();
        basicDataSource.setUsername(transactionConfig.userName);
        basicDataSource.setPassword(transactionConfig.password);
        basicDataSource.setDriverClassName(transactionConfig.driverClassName);
        basicDataSource.setInitialSize(transactionConfig.initialSize);
        basicDataSource.setMaxTotal(transactionConfig.maxTotal);
        basicDataSource.setMaxIdle(transactionConfig.maxIdle);
        basicDataSource.setMinIdle(transactionConfig.minIdle);
        basicDataSource.setMaxWaitMillis(transactionConfig.maxWaitMillis);

        basicDataSource.setUrl(transactionConfig.urls.get(0));

        setDataSource(basicDataSource);

        return true;
    }

    @Override
    public boolean shutdown() {
        try {
            ((BasicDataSource) getDataSource()).close();
        } catch (Exception e) {
            log.warn("", e);
        }

        return true;
    }
}
