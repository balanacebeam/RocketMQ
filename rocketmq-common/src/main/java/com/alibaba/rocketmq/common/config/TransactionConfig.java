package com.alibaba.rocketmq.common.config;

import java.util.List;

/**
 * Created by diwayou on 2015/10/20.
 */
public class TransactionConfig {

    public int checkTransactionLogMinutesBefore = 1;

    public int checkScheduleIntervalSeconds = 1200;

    public int checkSchedulePoolSize = 1;

    public int checkPageSize = 100;

    public StoreType storeType = StoreType.none;

    public String userName;

    public String password;

    public String driverClassName = "com.mysql.jdbc.Driver";

    public int initialSize = 2;

    public int maxTotal = 50;

    public int maxIdle = 20;

    public int minIdle = 2;

    public long maxWaitMillis = 10000;

    public List<String> urls;

    public int batchSize = 40;

    public boolean asyncTransactionLog = true;

    public int asyncQueueSize = 3000;

    public int concurrentPoolSize = 4;

    public int noConcurrentBatchSize = 10;

    public static enum StoreType {
        none, jdbc, sharding_jdbc, concurrent_jdbc
    }
}
