package com.alibaba.rocketmq.store.transaction.jdbc;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.jdbc.sharding.ShardContextHolder;
import com.alibaba.rocketmq.store.transaction.jdbc.sharding.ShardingDataSource;
import com.alibaba.rocketmq.store.transaction.jdbc.sharding.route.RouteUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.dbcp2.BasicDataSource;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by diwayou on 2015/10/21.
 */
public class ShardingJdbcTransactionStore extends AbstractJdbcTransactionStore {

    public ShardingJdbcTransactionStore(Config config) {
        super(config);
    }

    @Override
    public boolean start() {
        TransactionConfig transactionConfig = config.transactionConfig;

        if (CollectionUtils.isEmpty(transactionConfig.urls)) {
            log.error("must config urls in transaction config.");
            return false;
        }

        ShardingDataSource shardingDataSource = new ShardingDataSource();
        int index = 0;
        for (String url : transactionConfig.urls) {
            BasicDataSource basicDataSource = buildDataSource(transactionConfig);
            basicDataSource.setUrl(url);
            String dbName = RouteUtil.buildDbNameByIndex(index);

            shardingDataSource.addDataSource(dbName, basicDataSource);
            index++;
        }

        setDataSource(shardingDataSource);

        return true;
    }

    private BasicDataSource buildDataSource(TransactionConfig transactionConfig) {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setUsername(transactionConfig.userName);
        dataSource.setPassword(transactionConfig.password);
        dataSource.setDriverClassName(transactionConfig.driverClassName);
        dataSource.setInitialSize(transactionConfig.initialSize);
        dataSource.setMaxTotal(transactionConfig.maxTotal);
        dataSource.setMaxIdle(transactionConfig.maxIdle);
        dataSource.setMinIdle(transactionConfig.minIdle);
        dataSource.setMaxWaitMillis(transactionConfig.maxWaitMillis);

        return dataSource;
    }

    @Override
    public boolean shutdown() {
        try {
            ShardingDataSource shardingDataSource = (ShardingDataSource) getDataSource();
            for (Map.Entry<String, DataSource> entry : shardingDataSource.getDataSourceRoute().entrySet()) {
                BasicDataSource basicDataSource = (BasicDataSource) entry.getValue();
                basicDataSource.close();
            }
        } catch (Exception e) {
            // ignore
        }
        return true;
    }

    @Override
    public void remove(List<TransactionRecord> transactionRecordList) {
        try {
            if (CollectionUtils.isEmpty(transactionRecordList)) return;

            String groupName = transactionRecordList.get(0).getProducerGroup();
            String dbName = RouteUtil.buildDbNameByProducerGroup(groupName, config.transactionConfig.urls.size());

            ShardContextHolder.setShardDataSourceName(dbName);

            super.remove(transactionRecordList);
        } finally {
            ShardContextHolder.clearShardDataSourceName();
        }
    }

    @Override
    public Date getStoreTime(String groupName) {
        try {
            String dbName = RouteUtil.buildDbNameByProducerGroup(groupName, config.transactionConfig.urls.size());
            ShardContextHolder.setShardDataSourceName(dbName);

            return super.getStoreTime(groupName);
        } finally {
            ShardContextHolder.clearShardDataSourceName();
        }
    }

    @Override
    public List<TransactionRecord> traverse(Map<String, Object> context, TransactionRecord transactionRecord, int pageSize) {
        try {
            String groupName = transactionRecord.getProducerGroup();
            String dbName = RouteUtil.buildDbNameByProducerGroup(groupName, config.transactionConfig.urls.size());
            ShardContextHolder.setShardDataSourceName(dbName);

            return super.traverse(context, transactionRecord, pageSize);
        } finally {
            ShardContextHolder.clearShardDataSourceName();
        }
    }

    @Override
    public boolean put(List<TransactionRecord> transactionRecordList) {
        try {
            if (CollectionUtils.isEmpty(transactionRecordList)) return true;

            String groupName = transactionRecordList.get(0).getProducerGroup();
            String dbName = RouteUtil.buildDbNameByProducerGroup(groupName, config.transactionConfig.urls.size());
            ShardContextHolder.setShardDataSourceName(dbName);

            return super.put(transactionRecordList);
        } finally {
            ShardContextHolder.clearShardDataSourceName();
        }
    }
}
