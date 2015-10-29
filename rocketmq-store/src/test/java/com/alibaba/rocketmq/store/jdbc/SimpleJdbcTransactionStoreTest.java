package com.alibaba.rocketmq.store.jdbc;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import com.alibaba.rocketmq.common.config.YamlConfigurationLoader;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.jdbc.SimpleJdbcTransactionStore;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.lang3.time.DateUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SimpleJdbcTransactionStoreTest {

    private YamlConfigurationLoader yamlConfigurationLoader;

    private BasicDataSource dataSource;

    private SimpleJdbcTransactionStore simpleJdbcTransactionStore;

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

        simpleJdbcTransactionStore = new SimpleJdbcTransactionStore(config);
        simpleJdbcTransactionStore.setDataSource(dataSource);
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
        Date date = simpleJdbcTransactionStore.getStoreTime("");

        Assert.assertNotNull(date);
    }

    @Test
    public void putTest() {
        List<TransactionRecord> transactionRecordList = Lists.newArrayList();
        for (int i = 31; i < 60; i++) {
            transactionRecordList.add(buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, i));
        }

        boolean result = simpleJdbcTransactionStore.put(transactionRecordList);

        Assert.assertTrue(result);
    }

    @Test
    public void traverseTest() {
        TransactionRecord transactionRecord = buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, 0);
        transactionRecord.setGmtCreate(DateUtils.addMinutes(new Date(), -20));

        Map<String, Object> context = Maps.newHashMap();
        List<TransactionRecord> recordList = simpleJdbcTransactionStore.traverse(context, transactionRecord, 10);
        Long id = (Long) context.get(SimpleJdbcTransactionStore.ID);
        Assert.assertTrue(recordList != null);
        Assert.assertEquals(10, recordList.size());

        List<TransactionRecord> nextRecordList = simpleJdbcTransactionStore.traverse(context, transactionRecord, 10);
        Long nextId = (Long) context.get(SimpleJdbcTransactionStore.ID);
        Assert.assertTrue(nextRecordList != null);
        Assert.assertEquals(10, nextRecordList.size());
        Assert.assertNotEquals(id, nextId);
    }

    @Test
    public void removeTest() {
        List<TransactionRecord> transactionRecordList = Lists.newArrayList();
        for (int i = 51; i < 60; i++) {
            transactionRecordList.add(buildTransactionRecord(BROKER_NAME, PRODUCER_GROUP, i));
        }

        simpleJdbcTransactionStore.remove(transactionRecordList);
    }
}
