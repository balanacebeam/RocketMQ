package com.alibaba.rocketmq.common.config;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

/**
 * Created by diwayou on 2015/10/20.
 */
public class Config {

    public TransactionConfig transactionConfig;

    public String brokerName;

    public int indexBatchSize = 100;

    public int waitStrategy = 2;

    public boolean check() {
        if (transactionConfig != null && transactionConfig.storeType.equals(TransactionConfig.StoreType.none)) {
            if (StringUtils.isEmpty(brokerName)) {
                return false;
            }

            if (CollectionUtils.isEmpty(transactionConfig.urls)) {
                return false;
            }
        }

        return true;
    }
}
