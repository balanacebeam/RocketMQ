package com.alibaba.rocketmq.broker.transaction.jdbc;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import com.alibaba.rocketmq.common.config.YamlConfigurationLoader;
import com.google.common.collect.Lists;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;

public class DbTransactionStoreTest {

    private YamlConfigurationLoader yamlConfigurationLoader;

    private BasicDataSource dataSource;

    private DbTransactionStore dbTransactionStore;

    private static final String BROKER_NAME = "test_broker_name";

    private static final String PRODUCER_GROUP = "test_producer_group";

    @Before
    public void before() {
        yamlConfigurationLoader = new YamlConfigurationLoader();
        Config config = yamlConfigurationLoader.loadConfig();
        TransactionConfig transactionConfig = config.transactionConfig;

        dataSource = new BasicDataSource();
        dataSource.setUsername(transactionConfig.userName);
        dataSource.setPassword(transactionConfig.password);
        dataSource.setDriverClassName(transactionConfig.driverClassName);
        dataSource.setInitialSize(transactionConfig.initialSize);
        dataSource.setMaxTotal(transactionConfig.maxTotal);
        dataSource.setMaxIdle(transactionConfig.maxIdle);
        dataSource.setMinIdle(transactionConfig.minIdle);
        dataSource.setMaxWaitMillis(transactionConfig.maxWaitMillis);
        dataSource.setUrl(transactionConfig.urls.get(0));

        dbTransactionStore = new DbTransactionStore();
        dbTransactionStore.setDataSource(dataSource);
    }

    @After
    public void after() {
        try {
            dataSource.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private TransactionRecord buildTransactionRecord(String brokerName, String producerGroup, long offset) {
        TransactionRecord transactionRecord = new TransactionRecord();
        transactionRecord.setBrokerName(brokerName);
        transactionRecord.setProducerGroup(producerGroup);
        transactionRecord.setOffset(offset);

        return transactionRecord;
    }

    @Test
    public void getStoreTimeTest() {
        Date date = dbTransactionStore.getStoreTime();

        Assert.assertNotNull(date);
    }

    @Test
    public void putTest() {
        List<TransactionRecord> transactionRecordList = Lists.newArrayList();
        for (int i = 31; i < 60; i++) {
            transactionRecordList.add(buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, i));
        }

        boolean result = dbTransactionStore.put(transactionRecordList);

        Assert.assertTrue(result);
    }

    @Test
    public void traverseTest() {
        TransactionRecord transactionRecord = buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, 0);
        transactionRecord.setGmtCreate(DateUtils.addMinutes(new Date(), -20));

        List<TransactionRecord> recordList = dbTransactionStore.traverse(transactionRecord, 10);

        Assert.assertTrue(recordList != null);
        Assert.assertEquals(10, recordList.size());
    }

    @Test
    public void removeTest() {
        List<TransactionRecord> transactionRecordList = Lists.newArrayList();
        for (int i = 51; i < 60; i++) {
            transactionRecordList.add(buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, i));
        }

        dbTransactionStore.remove(transactionRecordList);
    }
}
