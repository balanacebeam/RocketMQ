package com.alibaba.rocketmq.store.transaction.ssdb;

import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.config.TransactionConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.TransactionStore;
import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.nutz.ssdb4j.SSDBs;
import org.nutz.ssdb4j.spi.Response;
import org.nutz.ssdb4j.spi.SSDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Created by diwayou on 2016/3/21.
 */
public class SSDBTransactionStore implements TransactionStore {

    protected static final Logger log = LoggerFactory.getLogger(LoggerName.TransactionLoggerName);

    private static final String KEY_NAMESPACE = "rmq-t-log";

    private static final String KEY_SEP = "_";
    private Config config;

    private SSDB ssdb;

    public SSDBTransactionStore(Config config) {
        this.config = config;
    }

    private static String buildLogKey(TransactionRecord transactionRecord) {
        return doBuildLogKey(transactionRecord.getBrokerName(), transactionRecord.getProducerGroup(),
                transactionRecord.getOffset());
    }

    private static String doBuildLogKey(String brokerName, String producerGroup, long offset) {
        return brokerName + KEY_SEP + producerGroup + KEY_SEP + offset;
    }

    private static boolean parseResponse(Response response) {
        return !response.asString().equals("false");
    }

    @Override
    public boolean start() {
        TransactionConfig transactionConfig = config.transactionConfig;

        if (CollectionUtils.isEmpty(transactionConfig.urls)) {
            log.error("must config urls in transaction config.");
            return false;
        }

        GenericObjectPool.Config config = new GenericObjectPool.Config();
        config.maxActive = transactionConfig.maxTotal;
        config.maxIdle = transactionConfig.maxIdle;
        config.maxWait = transactionConfig.maxWaitMillis;
        config.minEvictableIdleTimeMillis = 1000 * 10;
        config.minIdle = transactionConfig.minIdle;
        config.testOnBorrow = true;
        config.testOnReturn = true;
        config.testWhileIdle = true;

        URI uri = URI.create(transactionConfig.urls.get(0));
        ssdb = SSDBs.pool(uri.getHost(), uri.getPort(), (int) transactionConfig.maxWaitMillis, config);

        return true;
    }

    @Override
    public boolean shutdown() {
        if (ssdb != null) {
            try {
                ssdb.close();
            } catch (IOException e) {
                log.warn("shutdown error", e);
            }
        }

        return true;
    }

    @Override
    public boolean put(List<TransactionRecord> transactionRecordList) {
        try {
            List<String> paramList = Lists.newArrayList();
            for (TransactionRecord transactionRecord : transactionRecordList) {
                paramList.add(buildLogKey(transactionRecord));
                paramList.add(String.valueOf(transactionRecord.getGmtCreate().getTime()));
            }

            Response response = ssdb.multi_hset(KEY_NAMESPACE, paramList);
            response.check();

            return parseResponse(response);
        } catch (Exception e) {
            log.error("trans log put error: ", e);

            return false;
        }
    }

    @Override
    public void remove(List<TransactionRecord> transactionRecordList) {
        try {
            List<String> paramList = Lists.newArrayList();
            for (TransactionRecord transactionRecord : transactionRecordList) {
                paramList.add(buildLogKey(transactionRecord));
            }

            Response response = ssdb.multi_hdel(KEY_NAMESPACE, paramList);

            response.check();
        } catch (Exception e) {
            log.error("trans log remove error: ", e);
        }
    }

    @Override
    public Date getStoreTime(String producerGroup) {
        return new Date();
    }

    @Override
    public List<TransactionRecord> traverse(Map<String, Object> context, TransactionRecord transactionRecord, int pageSize) {
        try {
            final String OFFSET = "offset";
            Long lastOffset = (Long) context.get(OFFSET);
            if (lastOffset == null) {
                lastOffset = 0L;
            }

            Response response = ssdb.hscan(KEY_NAMESPACE,
                    doBuildLogKey(transactionRecord.getBrokerName(), transactionRecord.getProducerGroup(), lastOffset),
                    doBuildLogKey(transactionRecord.getBrokerName(), transactionRecord.getProducerGroup(), Long.MAX_VALUE),
                    pageSize);
            response.check();

            Map<String, String> logMap = response.mapString();

            List<TransactionRecord> result = Lists.newArrayListWithCapacity(logMap.size());

            for (Map.Entry<String, String> entry : logMap.entrySet()) {
                Date gmtCreate = new Date(Long.parseLong(entry.getValue()));
                if (gmtCreate.after(transactionRecord.getGmtCreate())) {
                    break;
                }

                String[] keys = entry.getKey().split(KEY_SEP);
                if (keys.length != 3) {
                    throw new RuntimeException("keys length is not 3. key=" + entry.getKey());
                }

                TransactionRecord record = new TransactionRecord();
                record.setBrokerName(keys[0]);
                record.setProducerGroup(keys[1]);
                record.setOffset(Long.parseLong(keys[2]));
                record.setGmtCreate(gmtCreate);

                result.add(record);
            }

            if (!result.isEmpty()) {
                context.put(OFFSET, result.get(result.size() - 1).getOffset());
            }

            return result;
        } catch (Exception e) {
            log.error("trans log traverse error: ", e);

            return Collections.EMPTY_LIST;
        }
    }
}
