package com.alibaba.rocketmq.store.dispatch.processor;

import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.dispatch.ValueEvent;
import com.google.common.collect.Lists;
import com.lmax.disruptor.EventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by diwayou on 2016/3/15.
 */
public class MessageIndexDispatchHandler implements EventHandler<ValueEvent> {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private final DefaultMessageStore defaultMessageStore;

    private List<DispatchRequest> indexBuffer;

    private int batchSize;

    public MessageIndexDispatchHandler(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.batchSize = defaultMessageStore.getConfig().indexBatchSize;
        this.indexBuffer = Lists.newArrayListWithCapacity(batchSize + 1);
    }

    @Override
    public void onEvent(ValueEvent event, long sequence, boolean endOfBatch) throws Exception {
        DispatchRequest req = event.getDispatchRequest();

        if (req == null) {
            log.warn("MessageIndexDispatchHandler event payload is null.");
            return;
        }
        event.setDispatchRequest(null);

        if (!this.defaultMessageStore.getMessageStoreConfig().isMessageIndexEnable()) {
            return;
        }

        if (endOfBatch) {
            indexBuffer.add(req);
        }

        if (indexBuffer.size() >= batchSize || endOfBatch) {
            this.defaultMessageStore.getIndexService().indexRequest(indexBuffer);

            indexBuffer.clear();
        }

        if (!endOfBatch) {
            indexBuffer.add(req);
        }
    }
}
