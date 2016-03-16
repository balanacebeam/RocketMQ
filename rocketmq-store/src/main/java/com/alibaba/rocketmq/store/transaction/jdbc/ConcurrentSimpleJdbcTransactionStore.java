package com.alibaba.rocketmq.store.transaction.jdbc;

import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.concurrent.*;

/**
 * 每个broker单库单表的时候使用
 */
public class ConcurrentSimpleJdbcTransactionStore extends SimpleJdbcTransactionStore {

    private ExecutorService executorService;

    private static final int ACTION_REMOVE = 1;

    private static final int ACTION_PUT = 2;

    public ConcurrentSimpleJdbcTransactionStore(Config config) {
        super(config);
    }

    @Override
    public void remove(List<TransactionRecord> transactionRecordList) {
        concurrentAction(transactionRecordList, ACTION_REMOVE);
    }

    @Override
    public boolean put(List<TransactionRecord> transactionRecordList) {
        return concurrentAction(transactionRecordList, ACTION_PUT);
    }

    private boolean concurrentAction(final List<TransactionRecord> transactionRecordList, final int action) {
        final int noConcurrentBatchSize = config.transactionConfig.noConcurrentBatchSize;
        int concurrentPoolSize = config.transactionConfig.concurrentPoolSize;

        if (transactionRecordList.size() <= noConcurrentBatchSize) {
            return doAction(transactionRecordList, action);
        }

        int concurrentNum = transactionRecordList.size() / noConcurrentBatchSize;
        if (transactionRecordList.size() % noConcurrentBatchSize != 0) {
            concurrentNum++;
        }
        if (concurrentNum <= concurrentPoolSize) {
            return doConcurrentAction(transactionRecordList, concurrentNum, noConcurrentBatchSize, action);
        }

        int batchSize = transactionRecordList.size() / concurrentPoolSize;

        return doConcurrentAction(transactionRecordList, concurrentPoolSize, batchSize, action);
    }

    private boolean doConcurrentAction(final List<TransactionRecord> transactionRecordList,
                                       final int concurrentNum,
                                       final int batchSize,
                                       final int action) {
        List<Future<Boolean>> results = Lists.newArrayListWithCapacity(concurrentNum);
        for (int i = 0; i < concurrentNum; i++) {
            final int beginIdx = i * batchSize;
            final int endIdx = i < concurrentNum - 1 ? (i + 1) * batchSize : transactionRecordList.size();
            results.add(executorService.submit(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    return doAction(transactionRecordList.subList(beginIdx, endIdx), action);
                }
            }));
        }

        for (Future<Boolean> result : results) {
            try {
                if (!result.get()) return false;
            } catch (InterruptedException e) {
                log.error("ConcurrentSimpleJdbcTransactionStore interrupted.", e);
                return false;
            } catch (ExecutionException e) {
                log.error("ConcurrentSimpleJdbcTransactionStore execution exception.", e);
                return false;
            }
        }

        return true;
    }

    private boolean doAction(List<TransactionRecord> transactionRecordList, int action) {
        switch (action) {
            case ACTION_PUT:
                return super.put(transactionRecordList);
            case ACTION_REMOVE:
                super.remove(transactionRecordList);
                return true;
            default:
                throw new IllegalArgumentException("ConcurrentSimpleJdbcTransactionStore action illegal. action=" + action);
        }
    }

    @Override
    public boolean start() {
        boolean result = super.start();

        if (result) {
            this.executorService = Executors.newFixedThreadPool(config.transactionConfig.concurrentPoolSize, new ThreadFactoryImpl("DBConcurrentPool"));
        }

        return result;
    }

    @Override
    public boolean shutdown() {
        boolean result = super.shutdown();

        if (this.executorService != null) {
            this.executorService.shutdownNow();
        }

        return result;
    }
}
