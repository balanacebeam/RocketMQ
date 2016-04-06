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
package com.alibaba.rocketmq.client.impl;

import com.alibaba.rocketmq.client.impl.factory.MQClientInstance;
import com.alibaba.rocketmq.client.impl.producer.MQProducerInner;
import com.alibaba.rocketmq.client.log.ClientLogger;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import com.alibaba.rocketmq.common.protocol.body.ConsumerRunningInfo;
import com.alibaba.rocketmq.common.protocol.body.GetConsumerStatusBody;
import com.alibaba.rocketmq.common.protocol.body.ResetOffsetBody;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.*;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-24
 */
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final Logger log = ClientLogger.getLog();
    private final MQClientInstance mqClientFactory;


    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }


    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        switch (request.getCode()) {
            case RequestCode.CHECK_TRANSACTION_STATE:
                return this.checkTransactionState(ctx, request);
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return this.notifyConsumerIdsChanged(ctx, request);
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return this.getConsumeStatus(ctx, request);

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            default:
                break;
        }
        return null;
    }


    private MessageCommand consumeMessageDirectly(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ConsumeMessageDirectlyResultRequestHeader requestHeader = request.getConsumeMessageDirectlyResultRequestHeader();

        final MessageExt msg = MessageDecoder.decode(request.getBody().asReadOnlyByteBuffer());

        ConsumeMessageDirectlyResult result =
                this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.getConsumerGroup(),
                        requestHeader.getBrokerName());

        if (null != result) {
            return CommandUtil.createResponseBuilder(request.getOpaque(), result.encode())
                    .build();
        } else {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    request.getOpaque(),
                    String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }
    }


    private MessageCommand getConsumerRunningInfo(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader = request.getGetConsumerRunningInfoRequestHeader();

        ConsumerRunningInfo consumerRunningInfo =
                this.mqClientFactory.consumerRunningInfo(requestHeader.getConsumerGroup());
        if (null != consumerRunningInfo) {
            if (requestHeader.getJstackEnable()) {
                String jstack = UtilAll.jstack();
                consumerRunningInfo.setJstack(jstack);
            }

            return CommandUtil.createResponseBuilder(request.getOpaque(), consumerRunningInfo.encode())
                    .build();
        } else {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(),
                    String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }
    }

    public MessageCommand checkTransactionState(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final CheckTransactionStateRequestHeader requestHeader = request.getCheckTransactionStateRequestHeader();
        final ByteBuffer byteBuffer = request.getBody().asReadOnlyByteBuffer();
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    public MessageCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        try {
            final NotifyConsumerIdsChangedRequestHeader requestHeader = request.getNotifyConsumerIdsChangedRequestHeader();
            log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",//
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()),//
                    requestHeader.getConsumerGroup());
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", UtilAll.exceptionSimpleDesc(e));
        }
        return null;
    }

    public MessageCommand resetOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ResetOffsetRequestHeader requestHeader = request.getResetOffsetRequestHeader();
        log.info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                        requestHeader.getGroup(), requestHeader.getTimestamp()});
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
        if (request.hasBody()) {
            ResetOffsetBody body = ResetOffsetBody.decode(request.getBody().toByteArray(), ResetOffsetBody.class);
            offsetTable = body.getOffsetTable();
        }
        this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
        return null;
    }

    @Deprecated
    public MessageCommand getConsumeStatus(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerStatusRequestHeader requestHeader = request.getGetConsumerStatusRequestHeader();

        Map<MessageQueue, Long> offsetTable =
                this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
        GetConsumerStatusBody body = new GetConsumerStatusBody();
        body.setMessageQueueTable(offsetTable);

        return CommandUtil.createResponseBuilder(request.getOpaque(), body.encode())
                .build();
    }
}
