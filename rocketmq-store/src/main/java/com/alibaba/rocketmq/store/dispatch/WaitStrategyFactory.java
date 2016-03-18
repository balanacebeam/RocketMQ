package com.alibaba.rocketmq.store.dispatch;

import com.lmax.disruptor.*;

/**
 * Created by diwayou on 2016/3/18.
 */
public class WaitStrategyFactory {

    public static WaitStrategy newInstance(int id) {
        switch (id) {
            case 0:
                return new SleepingWaitStrategy();
            case 1:
                return new YieldingWaitStrategy();
            case 2:
                return new BlockingWaitStrategy();
            case 3:
                return new BusySpinWaitStrategy();
            case 4:
                return new LiteBlockingWaitStrategy();
            default:
                throw new IllegalArgumentException("Wrong wait strategy id.");
        }
    }
}
