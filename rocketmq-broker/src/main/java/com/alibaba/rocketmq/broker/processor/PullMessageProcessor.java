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
import com.alibaba.rocketmq.broker.client.ConsumerGroupInfo;
import com.alibaba.rocketmq.broker.longpolling.PullRequest;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.pagecache.ManyMessageTransfer;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.filter.FilterAPI;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.heartbeat.MessageModel;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.TopicFilterType;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.protocol.topic.OffsetMovedEvent;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.PullSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.store.GetMessageResult;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.config.BrokerRole;
import io.netty.channel.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static com.alibaba.rocketmq.common.protocol.protobuf.Command.RpcType.RESPONSE;


/**
 * 拉消息请求处理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class PullMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;
    /**
     * 发送每条消息会回调
     */
    private List<ConsumeMessageHook> consumeMessageHookList;


    public PullMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public MessageCommand processRequest(final ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        MessageCommand.Builder responseBuiler = this.processRequest(ctx.channel(), request, true);

        return responseBuiler == null ? null : responseBuiler.build();
    }

    public void excuteRequestWhenWakeup(final Channel channel, final MessageCommand request)
            throws MessageCommandException {
        Runnable run = new Runnable() {
            @Override
            public void run() {
                try {
                    final MessageCommand.Builder responseBuilder =
                            PullMessageProcessor.this.processRequest(channel, request, false);

                    if (responseBuilder != null) {
                        responseBuilder.setOpaque(request.getOpaque());
                        responseBuilder.setRpcType(RESPONSE);
                        try {
                            channel.writeAndFlush(responseBuilder.build()).addListener(new ChannelFutureListener() {
                                @Override
                                public void operationComplete(ChannelFuture future) throws Exception {
                                    if (!future.isSuccess()) {
                                        log.error("processRequestWrapper response to "
                                                        + future.channel().remoteAddress() + " failed",
                                                future.cause());
                                        log.error(request.toString());
                                        log.error(responseBuilder.toString());
                                    }
                                }
                            });
                        } catch (Throwable e) {
                            log.error("processRequestWrapper process request over, but response failed", e);
                            log.error(request.toString());
                            log.error(responseBuilder.toString());
                        }
                    }
                } catch (MessageCommandException e1) {
                    log.error("excuteRequestWhenWakeup run", e1);
                }
            }
        };

        this.brokerController.getPullMessageExecutor().submit(run);
    }

    private void generateOffsetMovedEvent(final OffsetMovedEvent event) {
        try {
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setTopic(MixAll.OFFSET_MOVED_EVENT);
            msgInner.setTags(event.getConsumerGroup());
            msgInner.setDelayTimeLevel(0);
            msgInner.setKeys(event.getConsumerGroup());
            msgInner.setBody(event.encode());
            msgInner.setFlag(0);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgInner.getProperties()));
            msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(TopicFilterType.SINGLE_TAG,
                    msgInner.getTags()));

            msgInner.setQueueId(0);
            msgInner.setSysFlag(0);
            msgInner.setBornTimestamp(System.currentTimeMillis());
            msgInner.setBornHost(RemotingUtil.string2SocketAddress(this.brokerController.getBrokerAddr()));
            msgInner.setStoreHost(msgInner.getBornHost());

            msgInner.setReconsumeTimes(0);

            PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        } catch (Exception e) {
            log.warn(String.format("generateOffsetMovedEvent Exception, %s", event.toString()), e);
        }
    }

    private MessageCommand.Builder processRequest(final Channel channel, MessageCommand request,
                                          boolean brokerAllowSuspend) throws MessageCommandException {
        final PullMessageRequestHeader requestHeader = request.getPullMessageRequestHeader();

        MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (log.isDebugEnabled()) {
            log.debug("receive PullMessage request command, " + request);
        }

        // 检查Broker权限
        if (!PermName.isReadable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] pulling message is forbidden");
            return responseBuilder;
        }

        // 确保订阅组存在
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                        requestHeader.getConsumerGroup());
        if (null == subscriptionGroupConfig) {
            responseBuilder.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            responseBuilder.setRemark("subscription group not exist, " + requestHeader.getConsumerGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return responseBuilder;
        }

        // 这个订阅组是否可以消费消息
        if (!subscriptionGroupConfig.isConsumeEnable()) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark("subscription group no permission, " + requestHeader.getConsumerGroup());
            return responseBuilder;
        }

        final boolean hasSuspendFlag = PullSysFlag.hasSuspendFlag(requestHeader.getSysFlag());
        final boolean hasCommitOffsetFlag = PullSysFlag.hasCommitOffsetFlag(requestHeader.getSysFlag());
        final boolean hasSubscriptionFlag = PullSysFlag.hasSubscriptionFlag(requestHeader.getSysFlag());

        final long suspendTimeoutMillisLong = hasSuspendFlag ? requestHeader.getSuspendTimeoutMillis() : 0;

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            log.error("the topic " + requestHeader.getTopic() + " not exist, consumer: "
                    + RemotingHelper.parseChannelRemoteAddr(channel));
            responseBuilder.setCode(ResponseCode.TOPIC_NOT_EXIST);
            responseBuilder.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
            return responseBuilder;
        }

        // 检查topic权限
        if (!PermName.isReadable(topicConfig.getPerm())) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark("the topic[" + requestHeader.getTopic() + "] pulling message is forbidden");
            return responseBuilder;
        }

        // 检查队列有效性
        if (requestHeader.getQueueId() < 0 || requestHeader.getQueueId() >= topicConfig.getReadQueueNums()) {
            String errorInfo =
                    "queueId[" + requestHeader.getQueueId() + "] is illagal,Topic :"
                            + requestHeader.getTopic() + " topicConfig.readQueueNums: "
                            + topicConfig.getReadQueueNums() + " consumer: " + channel.remoteAddress();
            log.warn(errorInfo);
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(errorInfo);
            return responseBuilder;
        }

        // 订阅关系处理
        SubscriptionData subscriptionData = null;
        if (hasSubscriptionFlag) {
            try {
                subscriptionData =
                        FilterAPI.buildSubscriptionData(requestHeader.getConsumerGroup(),
                                requestHeader.getTopic(), requestHeader.getSubscription());
            } catch (Exception e) {
                log.warn("parse the consumer's subscription[{}] failed, group: {}",
                        requestHeader.getSubscription(),//
                        requestHeader.getConsumerGroup());
                responseBuilder.setCode(ResponseCode.SUBSCRIPTION_PARSE_FAILED);
                responseBuilder.setRemark("parse the consumer's subscription failed");
                return responseBuilder;
            }
        } else {
            ConsumerGroupInfo consumerGroupInfo =
                    this.brokerController.getConsumerManager().getConsumerGroupInfo(
                            requestHeader.getConsumerGroup());
            if (null == consumerGroupInfo) {
                log.warn("the consumer's group info not exist, group: {}", requestHeader.getConsumerGroup());
                responseBuilder.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                responseBuilder.setRemark("the consumer's group info not exist"
                        + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return responseBuilder;
            }

            if (!subscriptionGroupConfig.isConsumeBroadcastEnable() //
                    && consumerGroupInfo.getMessageModel() == MessageModel.BROADCASTING) {
                responseBuilder.setCode(ResponseCode.NO_PERMISSION);
                responseBuilder.setRemark("the consumer group[" + requestHeader.getConsumerGroup()
                        + "] can not consume by broadcast way");
                return responseBuilder;
            }

            subscriptionData = consumerGroupInfo.findSubscriptionData(requestHeader.getTopic());
            if (null == subscriptionData) {
                log.warn("the consumer's subscription not exist, group: {}", requestHeader.getConsumerGroup());
                responseBuilder.setCode(ResponseCode.SUBSCRIPTION_NOT_EXIST);
                responseBuilder.setRemark("the consumer's subscription not exist"
                        + FAQUrl.suggestTodo(FAQUrl.SAME_GROUP_DIFFERENT_TOPIC));
                return responseBuilder;
            }

            // 判断Broker的订阅关系版本是否最新
            if (subscriptionData.getSubVersion() < requestHeader.getSubVersion()) {
                log.warn("the broker's subscription is not latest, group: {} {}",
                        requestHeader.getConsumerGroup(), subscriptionData.getSubString());
                responseBuilder.setCode(ResponseCode.SUBSCRIPTION_NOT_LATEST);
                responseBuilder.setRemark("the consumer's subscription not latest");
                return responseBuilder;
            }
        }

        final GetMessageResult getMessageResult =
                this.brokerController.getMessageStore().getMessage(requestHeader.getConsumerGroup(),
                        requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getQueueOffset(),
                        requestHeader.getMaxMsgNums(), subscriptionData);
        if (getMessageResult != null) {
            PullMessageResponseHeader.Builder responseHeader = PullMessageResponseHeader.newBuilder()
                    .setNextBeginOffset(getMessageResult.getNextBeginOffset())
                    .setMinOffset(getMessageResult.getMinOffset())
                    .setMaxOffset(getMessageResult.getMaxOffset());

            responseBuilder.setRemark(getMessageResult.getStatus().name());
            responseBuilder.setPullMessageResponseHeader(responseHeader);

            // 消费较慢，重定向到另外一台机器
            if (getMessageResult.isSuggestPullingFromSlave()) {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig
                        .getWhichBrokerWhenConsumeSlowly());
            }
            // 消费正常，按照订阅组配置重定向
            else {
                responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
            }

            switch (getMessageResult.getStatus()) {
                case FOUND:
                    responseBuilder.setCode(ResponseCode.SUCCESS);

                    // 消息轨迹：记录客户端拉取的消息记录（不表示消费成功）
                    if (this.hasConsumeMessageHook()) {
                        // 执行hook
                        ConsumeMessageContext context = new ConsumeMessageContext();
                        context.setConsumerGroup(requestHeader.getConsumerGroup());
                        context.setTopic(requestHeader.getTopic());
                        context.setClientHost(RemotingHelper.parseChannelRemoteAddr(channel));
                        context.setStoreHost(this.brokerController.getBrokerAddr());
                        context.setQueueId(requestHeader.getQueueId());

                        final SocketAddress storeHost =
                                new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(),
                                        brokerController.getNettyServerConfig().getListenPort());
                        Map<String, Long> messageIds =
                                this.brokerController.getMessageStore().getMessageIds(requestHeader.getTopic(),
                                        requestHeader.getQueueId(), requestHeader.getQueueOffset(),
                                        requestHeader.getQueueOffset() + getMessageResult.getMessageCount(),
                                        storeHost);
                        context.setMessageIds(messageIds);
                        context.setBodyLength(getMessageResult.getBufferTotalSize()
                                / getMessageResult.getMessageCount());
                        this.executeConsumeMessageHookBefore(context);
                    }

                    break;
                case MESSAGE_WAS_REMOVING:
                    responseBuilder.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                // 这两个返回值都表示服务器暂时没有这个队列，应该立刻将客户端Offset重置为0
                case NO_MATCHED_LOGIC_QUEUE:
                case NO_MESSAGE_IN_QUEUE:
                    if (0 != requestHeader.getQueueOffset()) {
                        responseBuilder.setCode(ResponseCode.PULL_OFFSET_MOVED);

                        // XXX: warn and notify me
                        log.info(
                                "the broker store no queue data, fix the request offset {} to {}, Topic: {} QueueId: {} Consumer Group: {}",//
                                requestHeader.getQueueOffset(), //
                                getMessageResult.getNextBeginOffset(), //
                                requestHeader.getTopic(),//
                                requestHeader.getQueueId(),//
                                requestHeader.getConsumerGroup()//
                        );
                    } else {
                        responseBuilder.setCode(ResponseCode.PULL_NOT_FOUND);
                    }
                    break;
                case NO_MATCHED_MESSAGE:
                    responseBuilder.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    break;
                case OFFSET_FOUND_NULL:
                    responseBuilder.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_OVERFLOW_BADLY:
                    responseBuilder.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: " + requestHeader.getQueueOffset()
                            + " over flow badly, broker max offset: " + getMessageResult.getMaxOffset()
                            + ", consumer: " + channel.remoteAddress());
                    break;
                case OFFSET_OVERFLOW_ONE:
                    responseBuilder.setCode(ResponseCode.PULL_NOT_FOUND);
                    break;
                case OFFSET_TOO_SMALL:
                    responseBuilder.setCode(ResponseCode.PULL_OFFSET_MOVED);
                    // XXX: warn and notify me
                    log.info("the request offset: " + requestHeader.getQueueOffset()
                            + " too small, broker min offset: " + getMessageResult.getMinOffset()
                            + ", consumer: " + channel.remoteAddress());
                    break;
                default:
                    assert false;
                    break;
            }

            switch (responseBuilder.getCode()) {
                case ResponseCode.SUCCESS:
                    // 统计
                    this.brokerController.getBrokerStatsManager().incGroupGetNums(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getMessageCount());

                    this.brokerController.getBrokerStatsManager().incGroupGetSize(
                            requestHeader.getConsumerGroup(), requestHeader.getTopic(),
                            getMessageResult.getBufferTotalSize());

                    this.brokerController.getBrokerStatsManager().incBrokerGetNums(
                            getMessageResult.getMessageCount());

                    try {
                        FileRegion fileRegion =
                                new ManyMessageTransfer(ByteBuffer.wrap(responseBuilder.build().toByteArray()),
                                        getMessageResult);
                        channel.writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                            @Override
                            public void operationComplete(ChannelFuture future) throws Exception {
                                getMessageResult.release();
                                if (!future.isSuccess()) {
                                    log.error(
                                            "transfer many message by pagecache failed, " + channel.remoteAddress(),
                                            future.cause());
                                }
                            }
                        });
                    } catch (Throwable e) {
                        log.error("", e);
                        getMessageResult.release();
                    }

                    responseBuilder = null;
                    break;
                case ResponseCode.PULL_NOT_FOUND:
                    // 长轮询
                    if (brokerAllowSuspend && hasSuspendFlag) {
                        long pollingTimeMills = suspendTimeoutMillisLong;
                        if (!this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                            pollingTimeMills = this.brokerController.getBrokerConfig().getShortPollingTimeMills();
                        }

                        PullRequest pullRequest =
                                new PullRequest(request, channel, pollingTimeMills, this.brokerController
                                        .getMessageStore().now(), requestHeader.getQueueOffset());
                        this.brokerController.getPullRequestHoldService().suspendPullRequest(
                                requestHeader.getTopic(), requestHeader.getQueueId(), pullRequest);

                        responseBuilder = null;

                        break;
                    }

                    // 向Consumer返回应答
                case ResponseCode.PULL_RETRY_IMMEDIATELY:
                    break;
                case ResponseCode.PULL_OFFSET_MOVED:
                    if (this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE
                            || this.brokerController.getBrokerConfig().isOffsetCheckInSlave()) {
                        MessageQueue mq = new MessageQueue();
                        mq.setTopic(requestHeader.getTopic());
                        mq.setQueueId(requestHeader.getQueueId());
                        mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

                        OffsetMovedEvent event = new OffsetMovedEvent();
                        event.setConsumerGroup(requestHeader.getConsumerGroup());
                        event.setMessageQueue(mq);
                        event.setOffsetRequest(requestHeader.getQueueOffset());
                        event.setOffsetNew(getMessageResult.getNextBeginOffset());
                        this.generateOffsetMovedEvent(event);
                    } else {
                        responseHeader.setSuggestWhichBrokerId(subscriptionGroupConfig.getBrokerId());
                        responseBuilder.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                    }

                    log.warn(
                            "PULL_OFFSET_MOVED:topic={}, groupId={}, clientId={}, offset={}, suggestBrokerId={}",
                            requestHeader.getTopic(), requestHeader.getConsumerGroup(),
                            requestHeader.getQueueOffset(), responseHeader.getSuggestWhichBrokerId());
                    break;
                default:
                    assert false;
            }
        } else {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("store getMessage return null");
        }

        // 存储Consumer消费进度
        boolean storeOffsetEnable = brokerAllowSuspend; // 说明是首次调用，相对于长轮询通知
        storeOffsetEnable = storeOffsetEnable && hasCommitOffsetFlag; // 说明Consumer设置了标志位
        storeOffsetEnable = storeOffsetEnable // 只有Master支持存储offset
                && this.brokerController.getMessageStoreConfig().getBrokerRole() != BrokerRole.SLAVE;
        if (storeOffsetEnable) {
            this.brokerController.getConsumerOffsetManager().commitOffset(requestHeader.getConsumerGroup(),
                    requestHeader.getTopic(), requestHeader.getQueueId(), requestHeader.getCommitOffset());
        }

        return responseBuilder;
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }


    public void registerConsumeMessageHook(List<ConsumeMessageHook> sendMessageHookList) {
        this.consumeMessageHookList = sendMessageHookList;
    }


    public void executeConsumeMessageHookBefore(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageBefore(context);
                } catch (Throwable e) {
                }
            }
        }
    }
}
