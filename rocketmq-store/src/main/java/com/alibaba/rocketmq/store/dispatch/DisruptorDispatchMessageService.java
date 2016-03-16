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
import com.lmax.disruptor.SleepingWaitStrategy;
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

        int putMsgIndexHighWater = this.messageStoreConfig.getPutMsgIndexHightWater() * 2;
        this.ringBuffer = RingBuffer.createSingleProducer(ValueEvent.EVENT_FACTORY,
                UtilAll.roundPowOfTwo(putMsgIndexHighWater), new SleepingWaitStrategy());
        this.sequenceBarrier = this.ringBuffer.newBarrier();

        this.batchEventProcessors = new BatchEventProcessor[]{
                new BatchEventProcessor<>(this.ringBuffer, this.sequenceBarrier, new ConsumeQueueDispatchHandler(defaultMessageStore)),
                new BatchEventProcessor<>(this.ringBuffer, this.sequenceBarrier, new TransactionLogDispatchHandler(defaultMessageStore)),
                new BatchEventProcessor<>(this.ringBuffer, this.sequenceBarrier, new MessageIndexDispatchHandler(defaultMessageStore))
        };

        this.ringBuffer.addGatingSequences(this.batchEventProcessors[0].getSequence(),
                this.batchEventProcessors[1].getSequence(),
                this.batchEventProcessors[2].getSequence());
    }

    public void start() {
        this.executorService = Executors.newFixedThreadPool(this.batchEventProcessors.length, new ThreadFactoryImpl("DisruptorDispatchProcessor"));

        this.executorService.submit(this.batchEventProcessors[0]);
        this.executorService.submit(this.batchEventProcessors[1]);
        this.executorService.submit(this.batchEventProcessors[2]);
    }

    public void shutdown() {
        long start = System.currentTimeMillis();
        while (hasRemainMessage()) {
            if (System.currentTimeMillis() - start > 1000 * 60) break;

            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                // ignore
            }
        }

        this.executorService.shutdownNow();
    }


    public boolean hasRemainMessage() {
        if (this.ringBuffer.remainingCapacity() < this.ringBuffer.getBufferSize()) return true;

        return false;
    }


    public void putRequest(final DispatchRequest dispatchRequest) {
        long start = systemClock.now();
        long sequence = this.ringBuffer.next();

        try {
            this.ringBuffer.get(sequence).setDispatchRequest(dispatchRequest);
        } finally {
            this.ringBuffer.publish(sequence);
        }

        if (log.isInfoEnabled()) {
            log.info("DisruptorDispatchMessageService putRequest elapse=" + (systemClock.now() - start));
        }

        this.defaultMessageStore.getStoreStatsService().setDispatchMaxBuffer(this.ringBuffer.getBufferSize() - this.ringBuffer.remainingCapacity());

        if (log.isInfoEnabled() && this.ringBuffer.remainingCapacity() < 10000) {
            log.info("RingBuffer remainingCapacity" + " < 100");
        }
    }
}
