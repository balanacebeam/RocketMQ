package com.alibaba.rocketmq.common.config;

import java.util.List;

/**
 * Created by diwayou on 2015/10/20.
 */
public class TransactionConfig {

    public StoreType storeType;

    public String userName;

    public String password;

    public String driverClassName;

    public int initialSize;

    public int maxTotal;

    public int maxIdle;

    public int minIdle;

    public long maxWaitMillis;

    public List<String> urls;

    public static enum StoreType {
        jdbc
    }
}
