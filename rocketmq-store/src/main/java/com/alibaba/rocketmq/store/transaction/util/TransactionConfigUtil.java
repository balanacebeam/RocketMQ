package com.alibaba.rocketmq.store.transaction.util;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;

/**
 * Created by diwayou on 2016/1/25.
 */
public class TransactionConfigUtil {

    public static boolean isTransaction(Config config) {
        return config != null && config.transactionConfig != null &&
                !config.transactionConfig.storeType.equals(TransactionConfig.StoreType.none);
    }
}
