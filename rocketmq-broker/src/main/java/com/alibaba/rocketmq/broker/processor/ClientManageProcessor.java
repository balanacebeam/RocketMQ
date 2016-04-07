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
package com.alibaba.rocketmq.broker.processor;

import com.alibaba.rocketmq.broker.BrokerController;
import com.alibaba.rocketmq.broker.client.ClientChannelInfo;
import com.alibaba.rocketmq.broker.client.ConsumerGroupInfo;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.GetConsumerListByGroupResponseBody;
import com.alibaba.rocketmq.common.protocol.heartbeat.ConsumerData;
import com.alibaba.rocketmq.common.protocol.heartbeat.HeartbeatData;
import com.alibaba.rocketmq.common.protocol.heartbeat.ProducerData;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.*;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;


/**
 * Client注册与注销管理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class ClientManageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;
    /**
     * 消费每条消息会回调
     */
    private List<ConsumeMessageHook> consumeMessageHookList;


    public ClientManageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        switch (request.getCode()) {
            case RequestCode.HEART_BEAT:
                return this.heartBeat(ctx, request);
            case RequestCode.UNREGISTER_CLIENT:
                return this.unregisterClient(ctx, request);
            case RequestCode.GET_CONSUMER_LIST_BY_GROUP:
                return this.getConsumerListByGroup(ctx, request);
            // 更新Consumer Offset
            case RequestCode.UPDATE_CONSUMER_OFFSET:
                return this.updateConsumerOffset(ctx, request);
            case RequestCode.QUERY_CONSUMER_OFFSET:
                return this.queryConsumerOffset(ctx, request);
            default:
                break;
        }
        return null;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }


    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                }
            }
        }
    }


    private MessageCommand updateConsumerOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final UpdateConsumerOffsetRequestHeader requestHeader = request.getUpdateConsumerOffsetRequestHeader();

        // 消息轨迹：记录已经消费成功并提交 offset 的消息记录
        if (this.hasConsumeMessageHook()) {
            // 执行hook
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getConsumerGroup());
            context.setTopic(requestHeader.getTopic());
            context.setClientHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            context.setSuccess(true);
            context.setStatus(ConsumeConcurrentlyStatus.CONSUME_SUCCESS.toString());
            final SocketAddress storeHost =
                    new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                            .getNettyServerConfig().getListenPort());

            long preOffset =
                    this.brokerController.getConsumerOffsetManager().queryOffset(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            requestHeader.getQueueId());
            Map<String, Long> messageIds =
                    this.brokerController.getMessageStore().getMessageIds(requestHeader.getTopic(),
                            requestHeader.getQueueId(), preOffset, requestHeader.getCommitOffset(), storeHost);
            context.setMessageIds(messageIds);
            this.executeConsumeMessageHookAfter(context);
        }
        this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
                requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand queryConsumerOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final QueryConsumerOffsetRequestHeader requestHeader = request.getQueryConsumerOffsetRequestHeader();

        long offset =
                this.brokerController.getConsumerOffsetManager().queryOffset(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic(), requestHeader.getQueueId());


        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        // 订阅组存在
        if (offset >= 0) {
            responseBuilder.setQueryConsumerOffsetResponseHeader(QueryConsumerOffsetResponseHeader.newBuilder().setOffset(offset));
            responseBuilder.setCode(ResponseCode.SUCCESS);
        }
        // 订阅组不存在
        else {
            long minOffset =
                    this.brokerController.getMessageStore().getMinOffsetInQuque(requestHeader.getTopic(),
                            requestHeader.getQueueId());
            // 订阅组不存在情况下，如果这个队列的消息最小Offset是0，则表示这个Topic上线时间不长，服务器堆积的数据也不多，那么这个订阅组就从0开始消费。
            // 尤其对于Topic队列数动态扩容时，必须要从0开始消费。
            if (minOffset <= 0
                    && !this.brokerController.getMessageStore().checkInDiskByConsumeOffset(
                    requestHeader.getTopic(), requestHeader.getQueueId(), 0)) {
                responseBuilder.setQueryConsumerOffsetResponseHeader(QueryConsumerOffsetResponseHeader.newBuilder().setOffset(0));
                responseBuilder.setCode(ResponseCode.SUCCESS);
            }
            // 新版本服务器不做消费进度纠正
            else {
                responseBuilder.setCode(ResponseCode.QUERY_NOT_FOUND);
                responseBuilder.setRemark("Not found, V3_0_6_SNAPSHOT maybe this group consumer boot first");
            }
        }

        return responseBuilder.build();
    }


    public MessageCommand getConsumerListByGroup(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerListByGroupRequestHeader requestHeader = request.getGetConsumerListByGroupRequestHeader();

        ConsumerGroupInfo consumerGroupInfo = this.brokerController.getConsumerManager().getConsumerGroupInfo(
                        requestHeader.getConsumerGroup());

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (consumerGroupInfo != null) {
            List<String> clientIds = consumerGroupInfo.getAllClientId();
            if (!clientIds.isEmpty()) {
                GetConsumerListByGroupResponseBody body = new GetConsumerListByGroupResponseBody();
                body.setConsumerIdList(clientIds);

                byte[] bBody = body.encode();
                if (ArrayUtils.isNotEmpty(bBody)) {
                    responseBuilder.setBody(ByteString.copyFrom(bBody));
                }
                responseBuilder.setCode(ResponseCode.SUCCESS);

                return responseBuilder.build();
            } else {
                log.warn("getAllClientId failed, {} {}", requestHeader.getConsumerGroup(),
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            }
        } else {
            log.warn("getConsumerGroupInfo failed, {} {}", requestHeader.getConsumerGroup(),
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        }

        responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
        responseBuilder.setRemark("no consumer for this group, " + requestHeader.getConsumerGroup());

        return responseBuilder.build();
    }


    public MessageCommand unregisterClient(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final UnregisterClientRequestHeader requestHeader = request.getUnregisterClientRequestHeader();

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
                ctx.channel(),//
                requestHeader.getClientId(),//
                request.getVersion()//
        );

        // 注销Producer
        {
            final String group = requestHeader.getProducerGroup();
            if (group != null) {
                this.brokerController.getProducerManager().unregisterProducer(group, clientChannelInfo);
            }
        }

        // 注销Consumer
        {
            final String group = requestHeader.getConsumerGroup();
            if (group != null) {
                this.brokerController.getConsumerManager().unregisterConsumer(group, clientChannelInfo);
            }
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    public MessageCommand heartBeat(ChannelHandlerContext ctx, MessageCommand request) {
        HeartbeatData heartbeatData = HeartbeatData.decode(request.getBody().toByteArray(), HeartbeatData.class);

        ClientChannelInfo clientChannelInfo = new ClientChannelInfo(//
                ctx.channel(),//
                heartbeatData.getClientID(),//
                request.getVersion()//
        );

        // 注册Consumer
        for (ConsumerData data : heartbeatData.getConsumerDataSet()) {
            SubscriptionGroupConfig subscriptionGroupConfig =
                    this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                            data.getGroupName());
            if (null != subscriptionGroupConfig) {
                // 如果是单元化模式，则对 topic 进行设置
                int topicSysFlag = 0;
                if (data.isUnitMode()) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                }
                String newTopic = MixAll.getRetryTopic(data.getGroupName());
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
                        newTopic,//
                        subscriptionGroupConfig.getRetryQueueNums(), //
                        PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
            }

            boolean changed = this.brokerController.getConsumerManager().registerConsumer(//
                    data.getGroupName(),//
                    clientChannelInfo,//
                    data.getConsumeType(),//
                    data.getMessageModel(),//
                    data.getConsumeFromWhere(),//
                    data.getSubscriptionDataSet()//
            );

            if (changed) {
                log.info("registerConsumer info changed {} {}",//
                        data.toString(),//
                        RemotingHelper.parseChannelRemoteAddr(ctx.channel())//
                );

                // todo:有可能会有频繁变更
                // for (SubscriptionData subscriptionData :
                // data.getSubscriptionDataSet()) {
                // this.brokerController.getTopicConfigManager().updateTopicUnitSubFlag(
                // subscriptionData.getTopic(), data.isUnitMode());
                // }
            }
        }

        // 注册Producer
        for (ProducerData data : heartbeatData.getProducerDataSet()) {
            this.brokerController.getProducerManager().registerProducer(data.getGroupName(),
                    clientChannelInfo);
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }
}
