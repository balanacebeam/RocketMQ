package com.alibaba.rocketmq.store.dispatch;

import com.alibaba.rocketmq.store.DispatchRequest;
import com.lmax.disruptor.EventFactory;

/**
 * Created by diwayou on 2016/3/15.
 */
public class ValueEvent {

    private DispatchRequest dispatchRequest;

    public DispatchRequest getDispatchRequest() {
        return dispatchRequest;
    }

    public void setDispatchRequest(DispatchRequest dispatchRequest) {
        this.dispatchRequest = dispatchRequest;
    }

    public static final EventFactory<ValueEvent> EVENT_FACTORY = new EventFactory<ValueEvent>()
    {
        public ValueEvent newInstance()
        {
            return new ValueEvent();
        }
    };
}
