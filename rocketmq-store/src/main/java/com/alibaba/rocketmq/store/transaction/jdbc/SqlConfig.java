package com.alibaba.rocketmq.store.transaction.jdbc;

/**
 * Created by diwayou on 2015/10/20.
 */
public interface SqlConfig {

    String INSERT = "INSERT INTO t_transaction (id, broker_name, offset, producer_group, gmt_create) VALUES (NULL, ?, ?, ?, NOW())";

    String DELETE = "DELETE FROM t_transaction WHERE broker_name = ? and producer_group = ? and offset = ?";

    String GET_DB_TIME = "SELECT NOW()";

    String TRAVERSE = "SELECT id, offset FROM t_transaction WHERE broker_name = ? and producer_group = ? and gmt_create < ? and id < ? order by id desc limit ?";
}
