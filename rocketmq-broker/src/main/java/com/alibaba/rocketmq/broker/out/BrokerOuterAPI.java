/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.broker.out;

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.namesrv.TopAddressing;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.protocol.protobuf.NamesrvHeader.RegisterBrokerRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.NamesrvHeader.RegisterBrokerResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.NamesrvHeader.UnRegisterBrokerRequestHeader;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.RemotingClient;
import com.alibaba.rocketmq.remoting.exception.*;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Broker对外调用的API封装
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-7-3
 */
public class BrokerOuterAPI {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.WS_ADDR);
    private String nameSrvAddr = null;


    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig, RPCHook rpcHook) {
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
        this.remotingClient.registerRPCHook(rpcHook);
    }


    public BrokerOuterAPI(final NettyClientConfig nettyClientConfig) {
        this(nettyClientConfig, null);
    }


    public void start() {
        this.remotingClient.start();
    }


    public void shutdown() {
        this.remotingClient.shutdown();
    }


    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: " + this.nameSrvAddr + " new: " + addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }


    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        if (addrArray != null) {
            for (String addr : addrArray) {
                lst.add(addr);
            }

            this.remotingClient.updateNameServerAddressList(lst);
        }
    }


    private RegisterBrokerResult registerBroker(//
                                                final String namesrvAddr,//
                                                final String clusterName,// 1
                                                final String brokerAddr,// 2
                                                final String brokerName,// 3
                                                final long brokerId,// 4
                                                final String haServerAddr,// 5
                                                final TopicConfigSerializeWrapper topicConfigWrapper, // 6
                                                final List<String> filterServerList,// 7
                                                final boolean oneway// 8
    ) throws MessageCommandException, MQBrokerException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        RegisterBrokerRequestHeader requestHeader = RegisterBrokerRequestHeader.newBuilder()
                .setBrokerAddr(brokerAddr)
                .setBrokerId(brokerId)
                .setBrokerName(brokerName)
                .setClusterName(clusterName)
                .setHaServerAddr(haServerAddr)
                .build();

        RegisterBrokerBody requestBody = new RegisterBrokerBody();
        requestBody.setTopicConfigSerializeWrapper(topicConfigWrapper);
        requestBody.setFilterServerList(filterServerList);

        MessageCommand request = CommandUtil.createRequestBuiler(RequestCode.REGISTER_BROKER, requestBody.encode())
                .setRegisterBrokerRequestHeader(requestHeader)
                .build();

        if (oneway) {
            try {
                this.remotingClient.invokeOneway(namesrvAddr, request, 3000);
            } catch (RemotingTooMuchRequestException e) {
            }
            return null;
        }

        MessageCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                RegisterBrokerResponseHeader responseHeader = response.getRegisterBrokerResponseHeader();
                RegisterBrokerResult result = new RegisterBrokerResult();
                result.setMasterAddr(responseHeader.getMasterAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                result.setHaServerAddr(responseHeader.getHaServerAddr());
                if (response.hasBody()) {
                    result.setKvTable(KVTable.decode(response.getBody().toByteArray(), KVTable.class));
                }
                return result;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    public RegisterBrokerResult registerBrokerAll(//
                                                  final String clusterName,// 1
                                                  final String brokerAddr,// 2
                                                  final String brokerName,// 3
                                                  final long brokerId,// 4
                                                  final String haServerAddr,// 5
                                                  final TopicConfigSerializeWrapper topicConfigWrapper,// 6
                                                  final List<String> filterServerList,// 7
                                                  final boolean oneway// 8
    ) {
        RegisterBrokerResult registerBrokerResult = null;

        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    RegisterBrokerResult result =
                            this.registerBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId,
                                    haServerAddr, topicConfigWrapper, filterServerList, oneway);
                    if (result != null) {
                        registerBrokerResult = result;
                    }

                    log.info("register broker to name server {} OK", namesrvAddr);
                } catch (Exception e) {
                    log.warn("registerBroker Exception, " + namesrvAddr, e);
                }
            }
        }

        return registerBrokerResult;
    }


    public void unregisterBroker(//
                                 final String namesrvAddr,//
                                 final String clusterName,// 1
                                 final String brokerAddr,// 2
                                 final String brokerName,// 3
                                 final long brokerId// 4
    ) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException, MQBrokerException {
        UnRegisterBrokerRequestHeader requestHeader = UnRegisterBrokerRequestHeader.newBuilder()
                .setBrokerAddr(brokerAddr)
                .setBrokerId(brokerId)
                .setBrokerName(brokerName)
                .setClusterName(clusterName)
                .build();
        MessageCommand request = CommandUtil.createResponseBuilder(RequestCode.UNREGISTER_BROKER)
                .setUnRegisterBrokerRequestHeader(requestHeader)
                .build();

        MessageCommand response = this.remotingClient.invokeSync(namesrvAddr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return;
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    public void unregisterBrokerAll(//
                                    final String clusterName,// 1
                                    final String brokerAddr,// 2
                                    final String brokerName,// 3
                                    final long brokerId// 4
    ) {
        List<String> nameServerAddressList = this.remotingClient.getNameServerAddressList();
        if (nameServerAddressList != null) {
            for (String namesrvAddr : nameServerAddressList) {
                try {
                    this.unregisterBroker(namesrvAddr, clusterName, brokerAddr, brokerName, brokerId);
                    log.info("unregisterBroker OK, NamesrvAddr: {}", namesrvAddr);
                } catch (Exception e) {
                    log.warn("unregisterBroker Exception, " + namesrvAddr, e);
                }
            }
        }
    }


    public TopicConfigSerializeWrapper getAllTopicConfig(final String addr) throws RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException, InterruptedException, MQBrokerException {
        MessageCommand request = CommandUtil.createRequestCommand(RequestCode.GET_ALL_TOPIC_CONFIG);

        MessageCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return TopicConfigSerializeWrapper.decode(response.getBody().toByteArray(), TopicConfigSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取所有Consumer Offset
     *
     * @param addr
     * @return
     */
    public ConsumerOffsetSerializeWrapper getAllConsumerOffset(final String addr)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException {
        MessageCommand request = CommandUtil.createRequestCommand(RequestCode.GET_ALL_CONSUMER_OFFSET);
        MessageCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ConsumerOffsetSerializeWrapper.decode(response.getBody().toByteArray(),
                        ConsumerOffsetSerializeWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取所有定时进度
     *
     * @param addr
     * @return
     */
    public String getAllDelayOffset(final String addr) throws InterruptedException, RemotingTimeoutException,
            RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        MessageCommand request = CommandUtil.createRequestCommand(RequestCode.GET_ALL_DELAY_OFFSET);
        MessageCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return new String(response.getBody().toByteArray());
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    /**
     * 获取订阅组配置
     *
     * @param addr
     * @return
     */
    public SubscriptionGroupWrapper getAllSubscriptionGroupConfig(final String addr)
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException,
            RemotingConnectException, MQBrokerException {
        MessageCommand request = CommandUtil.createRequestCommand(RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG);
        MessageCommand response = this.remotingClient.invokeSync(addr, request, 3000);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return SubscriptionGroupWrapper.decode(response.getBody().toByteArray(), SubscriptionGroupWrapper.class);
            }
            default:
                break;
        }

        throw new MQBrokerException(response.getCode(), response.getRemark());
    }


    public void registerRPCHook(RPCHook rpcHook) {
        remotingClient.registerRPCHook(rpcHook);
    }
}
