package com.alibaba.rocketmq.common.protocol.body;

import com.alibaba.rocketmq.common.protocol.RemotingSerializable;

import java.util.List;

/**
 * Created by diwayou on 16-4-1.
 */
public class GetConsumerListByGroupResponseBody extends RemotingSerializable {
    private List<String> consumerIdList;


    public List<String> getConsumerIdList() {
        return consumerIdList;
    }


    public void setConsumerIdList(List<String> consumerIdList) {
        this.consumerIdList = consumerIdList;
    }
}
