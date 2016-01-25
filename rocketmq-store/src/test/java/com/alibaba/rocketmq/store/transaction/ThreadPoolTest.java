package com.alibaba.rocketmq.store.transaction;

import org.junit.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by diwayou on 2016/1/22.
 */
public class ThreadPoolTest {

    @Test
    public void rejectPolicyTest() {
        ExecutorService transactionExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(3));
        for (int i = 0; i < 10; i++) {
            try {
                transactionExecutor.execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            TimeUnit.HOURS.sleep(10);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
            } catch (Exception e) {
                System.out.println("i = " + i);
                e.printStackTrace();
                break;
            }
        }
    }
}
