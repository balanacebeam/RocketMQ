package com.alibaba.rocketmq.broker.transaction.jdbc;

/**
 * Created by diwayou on 2015/10/20.
 */
public interface SqlConfig {

    String INSERT = "INSERT INTO t_transaction (id, broker_name, offset, producer_group, gmt_create) VALUES (NULL, ?, ?, ?, NOW())";

    String DELETE = "DELETE FROM t_transaction WHERE broker_name = ? and producer_group = ? and offset = ?";

    String GET_DB_TIME = "SELECT NOW()";

    String TRAVERSE = "SELECT offset FROM t_transaction WHERE broker_name = ? and producer_group = ? and gmt_create < ? order by gmt_create desc limit ?";
}
