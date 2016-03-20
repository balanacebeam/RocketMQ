package com.alibaba.rocketmq.store.transaction.async;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2016/1/22.
 */
public class TransactionLogAsyncWorker implements java.lang.Runnable {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private List<TransactionRecord> prepareTransactionLog;

    private List<TransactionRecord> rollbackOrCommitTransactionLog;

    private long transactionTimestamp;

    private DefaultMessageStore defaultMessageStore;

    public TransactionLogAsyncWorker(List<TransactionRecord> prepareTransactionLog,
                                     List<TransactionRecord> rollbackOrCommitTransactionLog,
                                     long transactionTimestamp,
                                     DefaultMessageStore defaultMessageStore) {
        this.prepareTransactionLog = prepareTransactionLog;
        this.rollbackOrCommitTransactionLog = rollbackOrCommitTransactionLog;
        this.transactionTimestamp = transactionTimestamp;
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void run() {
        long start = this.defaultMessageStore.now();
        boolean result = this.defaultMessageStore.getTransactionStore().put(prepareTransactionLog);
        long elapse = this.defaultMessageStore.now() - start;
        if (elapse > 1000) {
            log.warn("doDispatch transaction put firstTime elapse={}", elapse);
        }

        int time = 1;
        while (!this.defaultMessageStore.isShutdown() && !result) { // retry until db recovery
            start = this.defaultMessageStore.now();
            result = this.defaultMessageStore.getTransactionStore().put(prepareTransactionLog);
            elapse = this.defaultMessageStore.now() - start;
            if (elapse > 1000) {
                log.warn("doDispatch transaction put elapse={}, times={}", elapse, time);
            }
            if (result) {
                log.warn("doDispatch transaction put retry success times={}.", time);
                break;
            }

            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
                // ignore
            }
            log.warn("doDispatch transaction put retry times={}", time);
            time++;
        }

        start = this.defaultMessageStore.now();
        this.defaultMessageStore.getTransactionStore().remove(rollbackOrCommitTransactionLog);
        elapse = this.defaultMessageStore.now() - start;
        if (elapse > 1000) {
            log.warn("doDispatch transaction remove elapse={}", elapse);
        }

        this.defaultMessageStore.getStoreCheckpoint().setTransactionTimestamp(transactionTimestamp);
    }
}
