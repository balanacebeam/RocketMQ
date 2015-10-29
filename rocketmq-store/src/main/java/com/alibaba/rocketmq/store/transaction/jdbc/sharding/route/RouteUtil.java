package com.alibaba.rocketmq.store.transaction.jdbc.sharding.route;

/**
 * Created by cn40387 on 15/10/28.
 */
public class RouteUtil {

    public static final String DEFAULT_DB_NAME = "rocketmq";

    public static final String SEPARATOR = "_";

    public static String buildDbName(String dbName, String suffix) {
        return dbName + SEPARATOR + suffix;
    }

    public static String formatRouteSuffix(long key) {
        return String.format("%04d", key);
    }

    public static String buildDbNameByProducerGroup(String producerGroup, int dbCount) {
        long index = Math.abs(producerGroup.hashCode()) % dbCount;
        return buildDbName(DEFAULT_DB_NAME, formatRouteSuffix(index));
    }

    public static String buildDbNameByIndex(int index) {
        return buildDbName(DEFAULT_DB_NAME, formatRouteSuffix(index));
    }
}
