package com.alibaba.rocketmq.broker.transaction.jdbc;

import com.alibaba.rocketmq.broker.transaction.TransactionRecord;
import com.alibaba.rocketmq.broker.transaction.TransactionStore;
import com.alibaba.rocketmq.common.constant.LoggerName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 使用broker_name和producer_group进行分库
 */
public class DbTransactionStore implements TransactionStore {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    private DataSource dataSource;

    @Override
    public void remove(List<TransactionRecord> transactionRecordList) {
        Connection connection = null;
        PreparedStatement statement = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);

            statement = connection.prepareStatement(SqlConfig.DELETE);
            for (TransactionRecord transactionRecord : transactionRecordList) {
                statement.setString(1, transactionRecord.getBrokerName());
                statement.setString(2, transactionRecord.getProducerGroup());
                statement.setLong(3, transactionRecord.getOffset());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            // ignore process result
            connection.commit();
        } catch (Exception e) {
            log.warn("DbTransactionStore remove", e);
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    @Override
    public Date getStoreTime() {
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.createStatement();

            resultSet = statement.executeQuery(SqlConfig.GET_DB_TIME);
            if (!resultSet.next()) {
                log.warn("DbTransactionStore getStoreTime ResultSet is empty.");
                return null;
            }

            return resultSet.getTimestamp(1);
        } catch (Exception e) {
            log.warn("DbTransactionStore getStoreTime", e);
            return null;
        } finally {
            closeResultSet(resultSet);
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    @Override
    public List<TransactionRecord> traverse(TransactionRecord transactionRecord, int pageSize) {
        PreparedStatement statement = null;
        Connection connection = null;
        ResultSet resultSet = null;
        try {
            connection = dataSource.getConnection();
            statement = connection.prepareStatement(SqlConfig.TRAVERSE);

            statement.setString(1, transactionRecord.getBrokerName());
            statement.setString(2, transactionRecord.getProducerGroup());
            statement.setTimestamp(3, new java.sql.Timestamp(transactionRecord.getGmtCreate().getTime()));
            statement.setInt(4, pageSize);

            resultSet = statement.executeQuery();

            List<TransactionRecord> result = new ArrayList<TransactionRecord>();
            while (resultSet.next()) {
                TransactionRecord tr = new TransactionRecord();
                tr.setBrokerName(transactionRecord.getBrokerName());
                tr.setProducerGroup(transactionRecord.getProducerGroup());
                tr.setOffset(resultSet.getLong(1));

                result.add(tr);
            }

            return result;
        } catch (Exception e) {
            log.warn("DbTransactionStore traverse", e);
            return new ArrayList<TransactionRecord>();
        } finally {
            closeResultSet(resultSet);
            closeStatement(statement);
            closeConnection(connection);
        }
    }


    @Override
    public boolean put(List<TransactionRecord> transactionRecordList) {
        PreparedStatement statement = null;
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            connection.setAutoCommit(false);
            statement = connection.prepareStatement(SqlConfig.INSERT);
            for (TransactionRecord tr : transactionRecordList) {
                statement.setString(1, tr.getBrokerName());
                statement.setLong(2, tr.getOffset());
                statement.setString(3, tr.getProducerGroup());
                statement.addBatch();
            }
            int[] executeBatch = statement.executeBatch();
            for (int affected : executeBatch) {
                if (affected != 1) {
                    return false;
                }
            }
            connection.commit();
            return true;
        } catch (Exception e) {
            log.warn("TransactionStore put", e);
            return false;
        } finally {
            closeStatement(statement);
            closeConnection(connection);
        }
    }

    private static void closeResultSet(ResultSet rs) {
        if (rs != null) {
            try {
                rs.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC ResultSet", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC ResultSet", e);
            }
        }
    }

    private static void closeStatement(Statement stmt) {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC Statement", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC Statement", e);
            }
        }
    }

    private static void closeConnection(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.debug("Could not close JDBC Connection", e);
            } catch (Throwable e) {
                log.debug("Unexpected exception on closing JDBC Connection", e);
            }
        }
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }
}
