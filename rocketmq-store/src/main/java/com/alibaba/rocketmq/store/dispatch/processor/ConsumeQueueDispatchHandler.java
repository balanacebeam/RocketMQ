package com.alibaba.rocketmq.store.dispatch.processor;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.dispatch.ValueEvent;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by diwayou on 2016/3/15.
 */
public class ConsumeQueueDispatchHandler implements EventHandler<ValueEvent> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private final DefaultMessageStore defaultMessageStore;

    public ConsumeQueueDispatchHandler(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    @Override
    public void onEvent(ValueEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.getDispatchRequest() == null) {
            log.warn("ConsumeQueueDispatchHandler event payload is null.");
            return;
        }

        DispatchRequest req = event.getDispatchRequest();

        final int tranType = MessageSysFlag.getTransactionValue(req.getSysFlag());

        // 1、分发消息位置信息到ConsumeQueue
        switch (tranType) {
            case MessageSysFlag.TransactionNotType:
            case MessageSysFlag.TransactionCommitType:
                // 将请求发到具体的Consume Queue
                defaultMessageStore.putMessagePostionInfo(req.getTopic(), req.getQueueId(),
                        req.getCommitLogOffset(), req.getMsgSize(), req.getTagsCode(),
                        req.getStoreTimestamp(), req.getConsumeQueueOffset());
                break;
            case MessageSysFlag.TransactionPreparedType:
            case MessageSysFlag.TransactionRollbackType:
                break;
        }
    }
}
