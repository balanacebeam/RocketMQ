package com.alibaba.rocketmq.store.dispatch;

import com.alibaba.rocketmq.common.SystemClock;
import com.alibaba.rocketmq.common.ThreadFactoryImpl;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.DispatchRequest;
import com.alibaba.rocketmq.store.config.MessageStoreConfig;
import com.alibaba.rocketmq.store.dispatch.processor.ConsumeQueueDispatchHandler;
import com.alibaba.rocketmq.store.dispatch.processor.MessageIndexDispatchHandler;
import com.alibaba.rocketmq.store.dispatch.processor.TransactionLogDispatchHandler;
import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2016/3/15.
 */
public class DisruptorDispatchMessageService {

    private static final Logger log = LoggerFactory.getLogger(LoggerName.StoreLoggerName);

    private final DefaultMessageStore defaultMessageStore;

    private final MessageStoreConfig messageStoreConfig;

    private final RingBuffer<ValueEvent> ringBuffer;

    private final SequenceBarrier sequenceBarrier;

    private final BatchEventProcessor<?>[] batchEventProcessors;

    private final SystemClock systemClock;

    private ExecutorService executorService;

    public DisruptorDispatchMessageService(DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
        this.messageStoreConfig = defaultMessageStore.getMessageStoreConfig();
        this.systemClock = defaultMessageStore.getSystemClock();

        int putMsgIndexHighWater = this.messageStoreConfig.getPutMsgIndexHightWater();
        this.ringBuffer = RingBuffer.createSingleProducer(ValueEvent.EVENT_FACTORY,
                UtilAll.roundPowOfTwo(putMsgIndexHighWater),
                WaitStrategyFactory.newInstance(this.defaultMessageStore.getConfig().waitStrategy));
        this.sequenceBarrier = this.ringBuffer.newBarrier();

        BatchEventProcessor<ValueEvent> consumeQueueProcessor = new BatchEventProcessor<>(this.ringBuffer, this.sequenceBarrier, new ConsumeQueueDispatchHandler(defaultMessageStore));
        BatchEventProcessor<ValueEvent> msgIdxProcessor = new BatchEventProcessor<>(this.ringBuffer, this.sequenceBarrier, new MessageIndexDispatchHandler(defaultMessageStore));
        BatchEventProcessor<ValueEvent> transLogProcessor = new BatchEventProcessor<>(this.ringBuffer,
                this.ringBuffer.newBarrier(consumeQueueProcessor.getSequence(), msgIdxProcessor.getSequence()),
                new TransactionLogDispatchHandler(defaultMessageStore));
        this.batchEventProcessors = new BatchEventProcessor[]{
                consumeQueueProcessor,
                msgIdxProcessor,
                transLogProcessor
        };

        this.ringBuffer.addGatingSequences(transLogProcessor.getSequence());
    }

    public void start() {
        this.executorService = Executors.newFixedThreadPool(this.batchEventProcessors.length, new ThreadFactoryImpl("DisruptorDispatchProcessor"));

        for (BatchEventProcessor processor : batchEventProcessors) {
            this.executorService.submit(processor);
        }
    }

    public void shutdown() {
        while (hasRemainMessage()) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        for (BatchEventProcessor batchEventProcessor : batchEventProcessors) {
            batchEventProcessor.halt();
        }

        this.executorService.shutdown();
        try {
            this.executorService.awaitTermination(24, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            log.error("DisruptorDispatchMessageService shutdown: ", e);
        }

        log.info("DisruptorDispatchMessageService shutdown success.");
    }


    public boolean hasRemainMessage() {
        if (this.ringBuffer.remainingCapacity() < this.ringBuffer.getBufferSize()) return true;

        return false;
    }


    public void putRequest(final DispatchRequest dispatchRequest) {
        long sequence = this.ringBuffer.next();

        try {
            this.ringBuffer.get(sequence).setDispatchRequest(dispatchRequest);
        } finally {
            this.ringBuffer.publish(sequence);
        }

        this.defaultMessageStore.getStoreStatsService().setDispatchMaxBuffer(this.ringBuffer.getBufferSize() - this.ringBuffer.remainingCapacity());

        if (log.isInfoEnabled() && this.ringBuffer.remainingCapacity() < 10000) {
            log.info("RingBuffer remainingCapacity" + " < 100");
        }
    }
}
