package com.alibaba.rocketmq.store.transaction.jdbc.sharding;

import com.google.common.collect.Maps;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;

/**
 * Created by diwayou on 2015/10/27.
 */
public class ShardingDataSource extends AbstractDataSource {

    private Map<String/*routeKey*/, DataSource> dataSourceRoute = Maps.newHashMap();

    @Override
    public Connection getConnection() throws SQLException {
        return getTargetDataSource().getConnection();
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return getTargetDataSource().getConnection(username, password);
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return (T) this;
        }
        return getTargetDataSource().unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return (iface.isInstance(this) || getTargetDataSource().isWrapperFor(iface));
    }

    private DataSource getTargetDataSource() {
        String routeKey = ShardContextHolder.getShardDataSourceName();

        DataSource dataSource = dataSourceRoute.get(routeKey);

        if (dataSource == null) {
            throw new IllegalStateException("Can't route DataSource for routeKey [" + routeKey + "]");
        }

        return dataSource;
    }

    public Map<String, DataSource> getDataSourceRoute() {
        return Collections.unmodifiableMap(dataSourceRoute);
    }

    public void addDataSource(String routeKey, DataSource dataSource) {
        if (dataSource == null) {
            throw new IllegalArgumentException("dataSource can't be null.");
        }

        dataSourceRoute.put(routeKey, dataSource);
    }

    public DataSource removeDataSource(String routeKey) {
        return dataSourceRoute.remove(routeKey);
    }
}
