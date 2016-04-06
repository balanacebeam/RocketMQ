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
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Random;

import static com.alibaba.rocketmq.common.protocol.protobuf.Command.RpcType.ONE_WAY;


/**
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    protected final SocketAddress storeHost;

    private List<SendMessageHook> sendMessageHookList;


    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost = new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                        .getNettyServerConfig().getListenPort());
    }

    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx, SendMessageRequestHeader requestHeader) {
        if (!this.hasSendMessageHook()) {
            return null;
        }
        SendMessageContext mqtraceContext;
        mqtraceContext = new SendMessageContext();
        mqtraceContext.setProducerGroup(requestHeader.getProducerGroup());
        mqtraceContext.setTopic(requestHeader.getTopic());
        mqtraceContext.setMsgProps(requestHeader.getProperties());
        mqtraceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        mqtraceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        return mqtraceContext;
    }

    protected SendMessageRequestHeader parseRequestHeader(MessageCommand request) throws MessageCommandException {

        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_MESSAGE:
                    requestHeader = request.getSendMessageRequestHeader();
            default:
                break;
        }
        return requestHeader;
    }

    protected MessageCommand msgCheck(final ChannelHandlerContext ctx, final SendMessageRequestHeader requestHeader,
                                       final MessageCommand.Builder responseBuilder) {
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
                && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");
            return responseBuilder.build();
        }

        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(errorMsg);
            return responseBuilder.build();
        }

        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.getUnitMode()) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic " + requestHeader.getTopic() + " not exist, producer: "
                    + ctx.channel().remoteAddress());
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(//
                    requestHeader.getTopic(), //
                    requestHeader.getDefaultTopic(), //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicConfig =
                            this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                                    requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                                    topicSysFlag);
                }
            }

            if (null == topicConfig) {
                responseBuilder.setCode(ResponseCode.TOPIC_NOT_EXIST);
                responseBuilder.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                        + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return responseBuilder.build();
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illagal, %s Producer: %s",//
                    queueIdInt,//
                    topicConfig.toString(),//
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(errorInfo);

            return responseBuilder.build();
        }

        return responseBuilder.build();
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }


    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    protected void doResponse(ChannelHandlerContext ctx, MessageCommand request, final MessageCommand response) {
        if (!request.getRpcType().equals(ONE_WAY)) {
            try {
                ctx.writeAndFlush(response);
            } catch (Throwable e) {
                log.error("SendMessageProcessor process request over, but response failed", e);
                log.error(request.toString());
                log.error(response.toString());
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final MessageCommand request,
                                             SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    final SendMessageRequestHeader requestHeader = request.getSendMessageRequestHeader();

                    context.setProducerGroup(requestHeader.getProducerGroup());
                    context.setTopic(requestHeader.getTopic());
                    context.setBodyLength(request.getBody().size());
                    context.setMsgProps(requestHeader.getProperties());
                    context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                    context.setBrokerAddr(this.brokerController.getBrokerAddr());
                    context.setQueueId(requestHeader.getQueueId());
                    hook.sendMessageBefore(context);
                    //TODO can't update this value
                    //requestHeader.setProperties(context.getMsgProps());
                } catch (Throwable e) {
                }
            }
        }
    }


    public void executeSendMessageHookAfter(final MessageCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader = response.getSendMessageResponseHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {

                }
            }
        }
    }


}
