package com.alibaba.rocketmq.broker.transaction;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.config.Config;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.CheckTransactionStateRequestHeader;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.alibaba.rocketmq.store.config.BrokerRole;
import com.alibaba.rocketmq.store.transaction.TransactionRecord;
import com.google.common.collect.Maps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2015/10/20.
 */
public class TransactionStateService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final Config config;

    private final BrokerController brokerController;

    private final DefaultMessageStore messageStore;
    private final Random random = new Random(System.currentTimeMillis());
    private ScheduledExecutorService scheduledExecutorService;
    private ExecutorService executorService;

    public TransactionStateService(Config config, BrokerController brokerController, DefaultMessageStore messageStore) {
        this.config = config;
        this.brokerController = brokerController;
        this.messageStore = messageStore;
    }

    public void start() {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryImpl("TransactionCheckSchedule"));
        executorService = Executors.newFixedThreadPool(config.transactionConfig.checkSchedulePoolSize, new ThreadFactoryImpl("TransactionCheckPool"));

        initScheduleTask();
    }

    private void initScheduleTask() {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {
            private final boolean slave = brokerController.getMessageStoreConfig().getBrokerRole() == BrokerRole.SLAVE;

            @Override
            public void run() {
                if (slave) {
                    return;
                }

                for (final String groupName : brokerController.getProducerManager().getGroupNameSet()) {
                    executorService.execute(new Runnable() {
                        @Override
                        public void run() {
                            List<ClientChannelInfo> clientChannelInfoList = brokerController.getProducerManager().getClientChannelInfo(groupName);
                            if (CollectionUtils.isEmpty(clientChannelInfoList)) {
                                log.warn("Can't find client for groupName: " + groupName);
                                return;
                            }

                            Date dbTime = messageStore.getTransactionStore().getStoreTime(groupName);
                            int pageSize = config.transactionConfig.checkPageSize;

                            final TransactionRecord record = new TransactionRecord();
                            record.setBrokerName(config.brokerName);
                            record.setProducerGroup(groupName);
                            record.setGmtCreate(DateUtils.addMinutes(dbTime, -config.transactionConfig.checkTransactionLogMinutesBefore));

                            List<TransactionRecord> recordList;
                            Map<String, Object> context = Maps.newHashMap();
                            do {
                                recordList = messageStore.getTransactionStore().traverse(context, record, pageSize);

                                for (TransactionRecord transactionRecord : recordList) {
                                    checkTransactionRecord(transactionRecord, clientChannelInfoList);
                                }
                            } while (CollectionUtils.size(recordList) >= pageSize);
                        }
                    });
                }
            }
        }, 60, config.transactionConfig.checkScheduleIntervalSeconds, TimeUnit.SECONDS);
    }

    private void checkTransactionRecord(TransactionRecord transactionRecord, List<ClientChannelInfo> clientChannelInfoList) {
        SelectMapedBufferResult selectMapedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(transactionRecord.getOffset());
        if (null == selectMapedBufferResult) {
            log.warn("check a producer transaction state, but not find message by commitLogOffset: {}, group: ",
                    transactionRecord.getOffset(), transactionRecord.getProducerGroup());

            // TODO remove transaction log?
            this.messageStore.getTransactionStore().remove(Arrays.asList(transactionRecord));
            return;
        }

        ClientChannelInfo clientChannelInfo = randomChoose(clientChannelInfoList);

        final CheckTransactionStateRequestHeader requestHeader = CheckTransactionStateRequestHeader.newBuilder()
                .setCommitLogOffset(transactionRecord.getOffset())
                .setTranStateTableOffset(transactionRecord.getOffset())
                .build(); // TODO useful now?

        this.brokerController.getBroker2Client().checkProducerTransactionState(
                clientChannelInfo.getChannel(), requestHeader, selectMapedBufferResult);
    }

    private ClientChannelInfo randomChoose(List<ClientChannelInfo> clientChannelInfoList) {
        int index = this.generateRandmonNum() % clientChannelInfoList.size();
        ClientChannelInfo info = clientChannelInfoList.get(index);

        return info;
    }

    private int generateRandmonNum() {
        int value = this.random.nextInt();

        if (value < 0) {
            value = Math.abs(value);
        }

        return value;
    }

    public void shutdown() {
        if (scheduledExecutorService != null) {
            scheduledExecutorService.shutdownNow();
        }

        if (executorService != null) {
            executorService.shutdownNow();
        }
    }
}
