package com.alibaba.rocketmq.store.dispatch.processor;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.dispatch.ValueEvent;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.alibaba.rocketmq.store.transaction.async.TransactionLogAsyncWorker;
import com.alibaba.rocketmq.store.transaction.util.TransactionConfigUtil;
import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LifecycleAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.*;

/**
 * Created by diwayou on 2016/3/15.
 */
public class TransactionLogDispatchHandler implements EventHandler<ValueEvent>, LifecycleAware {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private final DefaultMessageStore defaultMessageStore;

    private volatile boolean isSlave = true;

    private ExecutorService transactionExecutor;

    private int batchSize;

    private List<TransactionRecord> rollbackOrCommit;

    private List<TransactionRecord> prepare;

    private long transactionTimestamp;

    public TransactionLogDispatchHandler(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.isSlave = this.defaultMessageStore.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;
        this.batchSize = this.defaultMessageStore.getConfig().transactionConfig.batchSize;

        rollbackOrCommit = Lists.newArrayListWithCapacity(batchSize);
        prepare = Lists.newArrayListWithCapacity(batchSize);
    }

    @Override
    public void onEvent(ValueEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.getDispatchRequest() == null) {
            log.warn("TransactionLogDispatchHandler event payload is null.");
            return;
        }

        if (this.isSlave) return;

        DispatchRequest req = event.getDispatchRequest();

        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());

        if (MessageSysFlag.TransactionNotType == tranType) return;

        if (!TransactionConfigUtil.isTransaction(this.defaultMessageStore.getConfig())) {
            log.warn("TransactionLogDispatchHandler receive transaction message. but not config.");
            return;
        }

        if (endOfBatch) {
            batchOneMessage(req, tranType);
        }

        if (prepare.size() >= batchSize || rollbackOrCommit.size() >= batchSize || endOfBatch) {
            final List<TransactionRecord> finalPrepare = prepare;
            final List<TransactionRecord> finalRollbackOrCommit = rollbackOrCommit;

            prepare = Lists.newArrayListWithCapacity(batchSize);
            rollbackOrCommit = Lists.newArrayListWithCapacity(batchSize);

            TransactionLogAsyncWorker worker = new TransactionLogAsyncWorker(finalPrepare,
                    finalRollbackOrCommit, transactionTimestamp,
                    defaultMessageStore);
            if (this.defaultMessageStore.getConfig().transactionConfig.asyncTransactionLog) {
                while (true) {
                    try {
                        transactionExecutor.execute(worker);
                        break;
                    } catch (RejectedExecutionException ree) {
                        log.warn("AsyncTransactionLog queue full, reject new worker.");
                        try {
                            TimeUnit.SECONDS.sleep(5);
                        } catch (InterruptedException e) {
                            // ignore
                        }
                        continue;
                    }
                }
            } else {
                worker.run();
            }
        }

        if (!endOfBatch) {
            batchOneMessage(req, tranType);
        }
    }

    private void batchOneMessage(final DispatchRequest req, final int tranType) {
        this.transactionTimestamp = req.getStoreTimestamp();

        switch (tranType) {
            case MessageSysFlag.TransactionNotType:
                break;
            case MessageSysFlag.TransactionPreparedType:
                prepare.add(buildTransactionRecord(req, true));
                break;
            case MessageSysFlag.TransactionCommitType:
            case MessageSysFlag.TransactionRollbackType:
                rollbackOrCommit.add(buildTransactionRecord(req, false));
                break;
        }
    }

    private TransactionRecord buildTransactionRecord(DispatchRequest request, boolean prepare) {
        TransactionRecord transactionRecord = new TransactionRecord();
        transactionRecord.setBrokerName(this.defaultMessageStore.getConfig().brokerName);
        transactionRecord.setProducerGroup(request.getProducerGroup());
        if (prepare) {
            transactionRecord.setOffset(request.getCommitLogOffset());
        } else {
            transactionRecord.setOffset(request.getPreparedTransactionOffset());
        }

        return transactionRecord;
    }

    @Override
    public void onStart() {
        if (TransactionConfigUtil.isTransaction(this.defaultMessageStore.getConfig()) &&
                this.defaultMessageStore.getConfig().transactionConfig.asyncTransactionLog) {
            transactionExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<Runnable>(this.defaultMessageStore.getConfig().transactionConfig.asyncQueueSize),
                    new ThreadPoolExecutor.AbortPolicy());
        }
    }

    @Override
    public void onShutdown() {
        shutdownUntilFinished();
    }

    public void shutdownUntilFinished() {
        if (transactionExecutor != null) {
            transactionExecutor.shutdown();

            while (true) {
                try {
                    transactionExecutor.awaitTermination(24, TimeUnit.HOURS);

                    break;
                } catch (InterruptedException e) {
                    log.error("TransactionLogDispatchHandler shutdownUntilFinished:", e);
                }
            }
        }
    }
}
