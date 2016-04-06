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
import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.admin.ConsumeStats;
import com.alibaba.rocketmq.common.admin.OffsetWrapper;
import com.alibaba.rocketmq.common.admin.TopicOffset;
import com.alibaba.rocketmq.common.admin.TopicStatsTable;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageId;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RemotingSerializable;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.*;
import com.alibaba.rocketmq.common.protocol.heartbeat.SubscriptionData;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.*;
import com.alibaba.rocketmq.common.protocol.protobuf.Command;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.protocol.protobuf.FiltersrvHeader.RegisterFilterServerRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.FiltersrvHeader.RegisterFilterServerResponseHeader;
import com.alibaba.rocketmq.common.stats.StatsItem;
import com.alibaba.rocketmq.common.stats.StatsSnapshot;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.store.DefaultMessageStore;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import com.google.protobuf.ByteString;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.*;


/**
 * 管理类请求处理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @author manhong.yqd<manhong.yqd@taobao.com>
 * @since 2013-7-26
 */
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);
    private final BrokerController brokerController;


    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        switch (request.getCode()) {
            // 更新创建Topic
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
            // 删除Topic
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return this.deleteTopic(ctx, request);
            // 获取Topic配置
            case RequestCode.GET_ALL_TOPIC_CONFIG:
                return this.getAllTopicConfig(ctx, request);

            // 更新Broker配置 TODO 可能存在并发问题
            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(ctx, request);
            // 获取Broker配置
            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(ctx, request);

            // 根据时间查询Offset
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return this.searchOffsetByTimestamp(ctx, request);
            case RequestCode.GET_MAX_OFFSET:
                return this.getMaxOffset(ctx, request);
            case RequestCode.GET_MIN_OFFSET:
                return this.getMinOffset(ctx, request);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return this.getEarliestMsgStoretime(ctx, request);

            // 获取Broker运行时信息
            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return this.getBrokerRuntimeInfo(ctx, request);

            // 锁队列与解锁队列
            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, request);
            case RequestCode.UNLOCK_BATCH_MQ:
                return this.unlockBatchMQ(ctx, request);

            // 订阅组配置
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                return this.updateAndCreateSubscriptionGroup(ctx, request);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return this.getAllSubscriptionGroup(ctx, request);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return this.deleteSubscriptionGroup(ctx, request);

            // 统计信息，获取Topic统计信息
            case RequestCode.GET_TOPIC_STATS_INFO:
                return this.getTopicStatsInfo(ctx, request);

            // Consumer连接管理
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return this.getConsumerConnectionList(ctx, request);
            // Producer连接管理
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return this.getProducerConnectionList(ctx, request);

            // 查询消费进度，订阅组下的所有Topic
            case RequestCode.GET_CONSUME_STATS:
                return this.getConsumeStats(ctx, request);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return this.getAllConsumerOffset(ctx, request);

            // 定时进度
            case RequestCode.GET_ALL_DELAY_OFFSET:
                return this.getAllDelayOffset(ctx, request);

            // 调用客户端重置 offset
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return this.resetOffset(ctx, request);

            // 调用客户端订阅消息处理
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return this.getConsumerStatus(ctx, request);

            // 查询Topic被哪些消费者消费
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return this.queryTopicConsumeByWho(ctx, request);

            case RequestCode.REGISTER_FILTER_SERVER:
                return this.registerFilterServer(ctx, request);
            // 根据 topic 和 group 获取消息的时间跨度
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return this.queryConsumeTimeSpan(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return this.getSystemTopicListFromBroker(ctx, request);

            // 删除失效队列
            case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                return this.cleanExpiredConsumeQueue(request);

            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            // 查找被修正 offset (转发组件）
            case RequestCode.QUERY_CORRECTION_OFFSET:
                return this.queryCorrectionOffset(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            case RequestCode.CLONE_GROUP_OFFSET:
                return this.cloneGroupOffset(ctx, request);

            // 查看Broker统计信息
            case RequestCode.VIEW_BROKER_STATS_DATA:
                return ViewBrokerStatsData(ctx, request);
            default:
                break;
        }

        return null;
    }


    private MessageCommand ViewBrokerStatsData(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ViewBrokerStatsDataRequestHeader requestHeader = request.getViewBrokerStatsDataRequestHeader();
        DefaultMessageStore messageStore = (DefaultMessageStore) this.brokerController.getMessageStore();

        StatsItem statsItem =
                messageStore.getBrokerStatsManager().getStatsItem(requestHeader.getStatsName(),
                        requestHeader.getStatsKey());
        if (null == statsItem) {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(),
                    String.format("The stats <%s> <%s> not exist", requestHeader.getStatsName(),
                            requestHeader.getStatsKey()));
        }

        BrokerStatsData brokerStatsData = new BrokerStatsData();
        // 分钟
        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInMinute();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsMinute(it);
        }

        // 小时
        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInHour();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsHour(it);
        }

        // 天
        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInDay();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsDay(it);
        }

        return CommandUtil.createResponseBuilder(request.getOpaque(), brokerStatsData.encode())
                .build();
    }


    private MessageCommand callConsumer(//
                                        final int requestCode,//
                                        final MessageCommand request, //
                                        final String consumerGroup,//
                                        final String clientId) throws MessageCommandException {
        ClientChannelInfo clientChannelInfo =
                this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (null == clientChannelInfo) {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return responseBuilder.build();
        }

        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(String.format(
                    "The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT", //
                    clientId,//
                    MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return responseBuilder.build();
        }

        try {
            MessageCommand newRequest = CommandUtil.createRequestBuiler(requestCode, request.getBody().toByteArray())
                    .addAllExtFields(request.getExtFieldsList())
                    .build();

            MessageCommand consumerResponse =
                    this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(),
                            newRequest);
            return consumerResponse;
        } catch (RemotingTimeoutException e) {
            responseBuilder.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            responseBuilder.setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId,
                    UtilAll.exceptionSimpleDesc(e)));
            return responseBuilder.build();
        } catch (Exception e) {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup,
                    clientId, UtilAll.exceptionSimpleDesc(e)));
            return responseBuilder.build();
        }
    }


    private MessageCommand consumeMessageDirectly(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ConsumeMessageDirectlyResultRequestHeader requestHeader = request.getConsumeMessageDirectlyResultRequestHeader();

        MessageCommand.Builder requestBuilder = MessageCommand.newBuilder(request);
        requestBuilder.addExtFields(Command.StrPair.newBuilder().setKey("brokerName").setValue(this.brokerController.getBrokerConfig().getBrokerName()));

        SelectMapedBufferResult selectMapedBufferResult = null;
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(requestHeader.getMsgId());
            selectMapedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(messageId.getOffset());

            byte[] body = new byte[selectMapedBufferResult.getSize()];
            selectMapedBufferResult.getByteBuffer().get(body);
            if (ArrayUtils.isNotEmpty(body)) {
                requestBuilder.setBody(ByteString.copyFrom(body));
            }
        } catch (UnknownHostException e) {
        } finally {
            if (selectMapedBufferResult != null) {
                selectMapedBufferResult.release();
            }
        }

        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, requestBuilder.build(),
                requestHeader.getConsumerGroup(), requestHeader.getClientId());
    }


    /**
     * 调用Consumer，获取Consumer内存数据结构，为监控以及定位问题
     */
    private MessageCommand getConsumerRunningInfo(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader = request.getGetConsumerRunningInfoRequestHeader();

        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request,
                requestHeader.getConsumerGroup(), requestHeader.getClientId());
    }


    public MessageCommand cleanExpiredConsumeQueue(MessageCommand request) {
        log.warn("invoke cleanExpiredConsumeQueue start.");
        brokerController.getMessageStore().cleanExpiredConsumerQueue();
        log.warn("invoke cleanExpiredConsumeQueue end.");

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand registerFilterServer(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final RegisterFilterServerRequestHeader requestHeader = request.getRegisterFilterServerRequestHeader();

        this.brokerController.getFilterServerManager().registerFilterServer(ctx.channel(),
                requestHeader.getFilterServerAddr());

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setRegisterFilterServerResponseHeader(RegisterFilterServerResponseHeader.newBuilder()
                        .setBrokerId(this.brokerController.getBrokerConfig().getBrokerId())
                        .setBrokerName(this.brokerController.getBrokerConfig().getBrokerName()))
                .build();
    }


    private MessageCommand getConsumeStats(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumeStatsRequestHeader requestHeader = request.getGetConsumeStatsRequestHeader();

        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics = new HashSet<String>();
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics =
                    this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(
                            requestHeader.getConsumerGroup());
        } else {
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("consumeStats, topic config not exist, {}", topic);
                continue;
            }

            /**
             * Consumer不在线的时候，也允许查询消费进度
             */
            {
                SubscriptionData findSubscriptionData =
                        this.brokerController.getConsumerManager().findSubscriptionData(
                                requestHeader.getConsumerGroup(), topic);
                // 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
                if (null == findSubscriptionData //
                        && this.brokerController.getConsumerManager().findSubscriptionDataCount(
                        requestHeader.getConsumerGroup()) > 0) {
                    log.warn("consumeStats, the consumer group[{}], topic[{}] not exist",
                            requestHeader.getConsumerGroup(), topic);
                    continue;
                }
            }

            for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
                if (brokerOffset < 0)
                    brokerOffset = 0;

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                        requestHeader.getConsumerGroup(),//
                        topic,//
                        i);
                if (consumerOffset < 0)
                    consumerOffset = 0;

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);

                // 查询消费者最后一条消息对应的时间戳
                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp =
                            this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i,
                                    timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            long consumeTps =
                    (long) this.brokerController.getBrokerStatsManager().tpsGroupGetNums(
                            requestHeader.getConsumerGroup(), topic);

            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();

        return CommandUtil.createResponseBuilder(request.getOpaque(), body).build();
    }


    private MessageCommand getProducerConnectionList(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetProducerConnectionListRequestHeader requestHeader = request.getGetProducerConnectionListRequestHeader();

        ProducerConnection bodydata = new ProducerConnection();
        HashMap<Channel, ClientChannelInfo> channelInfoHashMap =
                this.brokerController.getProducerManager().getGroupChannelTable()
                        .get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();

            return CommandUtil.createResponseBuilder(request.getOpaque(), body).build();
        }

        return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(),
                "the producer group[" + requestHeader.getProducerGroup() + "] not exist");
    }


    private MessageCommand getConsumerConnectionList(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerConnectionListRequestHeader requestHeader = request.getGetConsumerConnectionListRequestHeader();

        ConsumerGroupInfo consumerGroupInfo =
                this.brokerController.getConsumerManager().getConsumerGroupInfo(
                        requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            Iterator<Map.Entry<Channel, ClientChannelInfo>> it =
                    consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();

            return CommandUtil.createResponseBuilder(request.getOpaque(), body).build();
        }

        return CommandUtil.createResponseCommand(ResponseCode.CONSUMER_NOT_ONLINE, request.getOpaque(),
                "the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
    }


    private MessageCommand getTopicStatsInfo(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetTopicStatsInfoRequestHeader requestHeader = request.getGetTopicStatsInfoRequestHeader();

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            return CommandUtil.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, request.getOpaque(),
                    "topic[" + topic + "] not exist");
        }

        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQuque(topic, i);
            if (min < 0)
                min = 0;

            long max = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
            if (max < 0)
                max = 0;

            long timestamp = 0;
            if (max > 0) {
                timestamp =
                        this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, (max - 1));
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();

        return CommandUtil.createResponseBuilder(request.getOpaque(), body).build();
    }


    private MessageCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx,
                                                            MessageCommand request) throws MessageCommandException {
        log.info("updateAndCreateSubscriptionGroup called by {}",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config =
                RemotingSerializable.decode(request.getBody().toByteArray(), SubscriptionGroupConfig.class);
        if (config != null) {
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand getAllSubscriptionGroup(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        String content = this.brokerController.getSubscriptionGroupManager().encode();

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (content != null && content.length() > 0) {
            try {
                responseBuilder.setBody(ByteString.copyFrom(content, MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("UnsupportedEncodingException " + e);
                return responseBuilder.build();
            }
        } else {
            log.error("No subscription group in this broker, client: " + ctx.channel().remoteAddress());
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("No subscription group in this broker");
            return responseBuilder.build();
        }

        responseBuilder.setCode(ResponseCode.SUCCESS);

        return responseBuilder.build();
    }


    private MessageCommand lockBatchMQ(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        LockBatchRequestBody requestBody =
                LockBatchRequestBody.decode(request.getBody().toByteArray(), LockBatchRequestBody.class);

        Set<MessageQueue> lockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(//
                requestBody.getConsumerGroup(),//
                requestBody.getMqSet(),//
                requestBody.getClientId());

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        return CommandUtil.createResponseBuilder(request.getOpaque(), responseBody.encode())
                .build();
    }


    private MessageCommand unlockBatchMQ(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        UnlockBatchRequestBody requestBody =
                UnlockBatchRequestBody.decode(request.getBody().toByteArray(), UnlockBatchRequestBody.class);

        this.brokerController.getRebalanceLockManager().unlockBatch(//
                requestBody.getConsumerGroup(),//
                requestBody.getMqSet(),//
                requestBody.getClientId());

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand updateAndCreateTopic(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final CreateTopicRequestHeader requestHeader = request.getCreateTopicRequestHeader();
        log.info("updateAndCreateTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        // Topic名字是否与保留字段冲突
        if (requestHeader.getTopic().equals(this.brokerController.getBrokerConfig().getBrokerClusterName())) {
            String errorMsg =
                    "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);

            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(), errorMsg);
        }

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterType());
        topicConfig.setPerm(requestHeader.getPerm());
        topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag());

        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        this.brokerController.registerBrokerAll(false, true);

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand deleteTopic(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        DeleteTopicRequestHeader requestHeader = request.getDeleteTopicRequestHeader();

        log.info("deleteTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getTopicConfigManager().deleteTopicConfig(requestHeader.getTopic());
        this.brokerController.addDeleteTopicTask();

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand getAllTopicConfig(ChannelHandlerContext ctx, MessageCommand request) {
        String content = this.brokerController.getTopicConfigManager().encode();

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (content != null && content.length() > 0) {
            try {
                responseBuilder.setBody(ByteString.copyFrom(content, MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("UnsupportedEncodingException " + e);
                return responseBuilder.build();
            }
        } else {
            log.error("No topic in this broker, client: " + ctx.channel().remoteAddress());
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("No topic in this broker");
            return responseBuilder.build();
        }

        responseBuilder.setCode(ResponseCode.SUCCESS);

        return responseBuilder.build();
    }


    private MessageCommand updateBrokerConfig(ChannelHandlerContext ctx, MessageCommand request) {
        log.info("updateBrokerConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody().toByteArray();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    log.info("updateBrokerConfig, new config: " + properties + " client: "
                            + ctx.channel().remoteAddress());
                    this.brokerController.updateAllConfig(properties);
                } else {
                    log.error("string2Properties error");

                    return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(),
                            "string2Properties error");
                }
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR, request.getOpaque(),
                        "UnsupportedEncodingException " + e);
            }
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    private MessageCommand getBrokerConfig(ChannelHandlerContext ctx, MessageCommand request) {

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());

        String content = this.brokerController.encodeAllConfig();
        if (content != null && content.length() > 0) {
            try {
                responseBuilder.setBody(ByteString.copyFrom(content, MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("UnsupportedEncodingException " + e);
                return responseBuilder.build();
            }
        }

        responseBuilder.setGetBrokerConfigResponseHeader(GetBrokerConfigResponseHeader.newBuilder()
                .setVersion(this.brokerController.getConfigDataVersion()));

        responseBuilder.setCode(ResponseCode.SUCCESS);

        return responseBuilder.build();
    }


    private MessageCommand searchOffsetByTimestamp(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final SearchOffsetRequestHeader requestHeader = request.getSearchOffsetRequestHeader();

        long offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(),
                requestHeader.getQueueId(), requestHeader.getTimestamp());

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setSearchOffsetResponseHeader(SearchOffsetResponseHeader.newBuilder().setOffset(offset))
                .build();
    }


    private MessageCommand getMaxOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetMaxOffsetRequestHeader requestHeader = request.getGetMaxOffsetRequestHeader();

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQuque(requestHeader.getTopic(),
                requestHeader.getQueueId());

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setGetMaxOffsetResponseHeader(GetMaxOffsetResponseHeader.newBuilder().setOffset(offset))
                .build();
    }


    private MessageCommand getMinOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetMinOffsetRequestHeader requestHeader = request.getGetMinOffsetRequestHeader();

        long offset = this.brokerController.getMessageStore().getMinOffsetInQuque(requestHeader.getTopic(),
                requestHeader.getQueueId());

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setGetMinOffsetResponseHeader(GetMinOffsetResponseHeader.newBuilder().setOffset(offset))
                .build();
    }


    private MessageCommand getEarliestMsgStoretime(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetEarliestMsgStoreTimeRequestHeader requestHeader = request.getGetEarliestMsgStoreTimeRequestHeader();

        long timestamp =
                this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(),
                        requestHeader.getQueueId());

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setGetEarliestMsgStoreTimeResponseHeader(GetEarliestMsgStoreTimeResponseHeader.newBuilder()
                        .setTimestamp(timestamp))
                .build();
    }


    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CurrentVersion));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CurrentVersion));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow",
                String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning",
                String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow",
                String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("sendThreadPoolQueueSize",
                String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));

        runtimeInfo.put("sendThreadPoolQueueCapacity",
                String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        return runtimeInfo;
    }


    private MessageCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, MessageCommand request) {
        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        byte[] body = kvTable.encode();

        return CommandUtil.createResponseBuilder(request.getOpaque(), body).build();
    }


    private MessageCommand getAllConsumerOffset(ChannelHandlerContext ctx, MessageCommand request) {
        String content = this.brokerController.getConsumerOffsetManager().encode();

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (content != null && content.length() > 0) {
            try {
                responseBuilder.setBody(ByteString.copyFrom(content, MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all consumer offset from master error.", e);

                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("UnsupportedEncodingException " + e);
                return responseBuilder.build();
            }
        } else {
            log.error("No consumer offset in this broker, client: " + ctx.channel().remoteAddress());
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("No consumer offset in this broker");
            return responseBuilder.build();
        }

        responseBuilder.setCode(ResponseCode.SUCCESS);

        return responseBuilder.build();
    }


    private MessageCommand getAllDelayOffset(ChannelHandlerContext ctx, MessageCommand request) {
        String content = ((DefaultMessageStore) this.brokerController.getMessageStore()).getScheduleMessageService().encode();

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (content != null && content.length() > 0) {
            try {
                responseBuilder.setBody(ByteString.copyFrom(content, MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all delay offset from master error.", e);

                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("UnsupportedEncodingException " + e);
                return responseBuilder.build();
            }
        } else {
            log.error("No delay offset in this broker, client: " + ctx.channel().remoteAddress());
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("No delay offset in this broker");
            return responseBuilder.build();
        }

        responseBuilder.setCode(ResponseCode.SUCCESS);

        return responseBuilder.build();
    }


    private MessageCommand deleteSubscriptionGroup(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        DeleteSubscriptionGroupRequestHeader requestHeader = request.getDeleteSubscriptionGroupRequestHeader();

        log.info("deleteSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(
                requestHeader.getGroupName());

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    public MessageCommand resetOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ResetOffsetRequestHeader requestHeader = request.getResetOffsetRequestHeader();
        log.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                        requestHeader.getGroup(), requestHeader.getTimestamp(), requestHeader.getIsForce()});
        return this.brokerController.getBroker2Client().resetOffset(request, requestHeader.getTopic(),
                requestHeader.getGroup(), requestHeader.getTimestamp(), requestHeader.getIsForce());
    }


    public MessageCommand getConsumerStatus(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final GetConsumerStatusRequestHeader requestHeader = request.getGetConsumerStatusRequestHeader();

        log.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
                new Object[]{RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(),
                        requestHeader.getGroup()});

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(),
                requestHeader.getGroup(), requestHeader.getClientAddr());
    }


    private MessageCommand queryTopicConsumeByWho(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        QueryTopicConsumeByWhoRequestHeader requestHeader = request.getQueryTopicConsumeByWhoRequestHeader();

        // 从订阅关系查询topic被谁消费，只查询在线
        HashSet<String> groups =
                this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());
        // 从Offset持久化查询topic被谁消费，离线和在线都会查询
        Set<String> groupInOffset =
                this.brokerController.getConsumerOffsetManager().whichGroupByTopic(requestHeader.getTopic());
        if (groupInOffset != null && !groupInOffset.isEmpty()) {
            groups.addAll(groupInOffset);
        }

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        return CommandUtil.createResponseBuilder(request.getOpaque(), body)
                .build();
    }


    private MessageCommand queryConsumeTimeSpan(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        QueryConsumeTimeSpanRequestHeader requestHeader = request.getQueryConsumeTimeSpanRequestHeader();

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            return CommandUtil.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST, request.getOpaque(),
                    "topic[" + topic + "] not exist");
        }

        Set<QueueTimeSpan> timeSpanSet = new HashSet<QueueTimeSpan>();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            QueueTimeSpan timeSpan = new QueueTimeSpan();
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            timeSpan.setMessageQueue(mq);

            long minTime = this.brokerController.getMessageStore().getEarliestMessageTime(topic, i);
            timeSpan.setMinTimeStamp(minTime);

            long max = this.brokerController.getMessageStore().getMaxOffsetInQuque(topic, i);
            long maxTime =
                    this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, (max - 1));
            timeSpan.setMaxTimeStamp(maxTime);

            long consumeTime;
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(//
                    requestHeader.getGroup(), topic, i);
            if (consumerOffset > 0) {
                consumeTime =
                        this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i,
                                consumerOffset);
            } else {
                consumeTime = minTime;
            }
            timeSpan.setConsumeTimeStamp(consumeTime);
            timeSpanSet.add(timeSpan);
        }

        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(timeSpanSet);

        return CommandUtil.createResponseBuilder(request.getOpaque(), queryConsumeTimeSpanBody.encode())
                .build();
    }


    private MessageCommand getSystemTopicListFromBroker(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        Set<String> topics = this.brokerController.getTopicConfigManager().getSystemTopic();
        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);

        return CommandUtil.createResponseBuilder(request.getOpaque(), topicList.encode()).build();
    }


    private MessageCommand queryCorrectionOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        QueryCorrectionOffsetHeader requestHeader = request.getQueryCorrectionOffsetHeader();

        Map<Integer, Long> correctionOffset =
                this.brokerController.getConsumerOffsetManager().queryMinOffsetInAllGroup(
                        requestHeader.getTopic(), requestHeader.getFilterGroups());

        Map<Integer, Long> compareOffset =
                this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getTopic(),
                        requestHeader.getCompareGroup());

        if (compareOffset != null && !compareOffset.isEmpty()) {
            for (Integer queueId : compareOffset.keySet()) {
                correctionOffset.put(queueId,
                        correctionOffset.get(queueId) > compareOffset.get(queueId) ? Long.MAX_VALUE
                                : correctionOffset.get(queueId));
            }
        }

        QueryCorrectionOffsetBody body = new QueryCorrectionOffsetBody();
        body.setCorrectionOffsets(correctionOffset);

        return CommandUtil.createResponseBuilder(request.getOpaque(), body.encode())
                .build();
    }


    private MessageCommand cloneGroupOffset(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        CloneGroupOffsetRequestHeader requestHeader = request.getCloneGroupOffsetRequestHeader();

        Set<String> topics;
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(
                            requestHeader.getSrcGroup());
        } else {
            topics = new HashSet<String>();
            topics.add(requestHeader.getTopic());
        }

        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            /**
             * Consumer不在线的时候，也允许查询消费进度
             */
            if (!requestHeader.getOffline()) {
                // 如果Consumer在线，而且这个topic没有被订阅，那么就跳过
                SubscriptionData findSubscriptionData =
                        this.brokerController.getConsumerManager().findSubscriptionData(
                                requestHeader.getSrcGroup(), topic);
                if (this.brokerController.getConsumerManager().findSubscriptionDataCount(
                        requestHeader.getSrcGroup()) > 0
                        && findSubscriptionData == null) {
                    log.warn("[cloneGroupOffset], the consumer group[{}], topic[{}] not exist",
                            requestHeader.getSrcGroup(), topic);
                    continue;
                }
            }

            this.brokerController.getConsumerOffsetManager().cloneOffset(requestHeader.getSrcGroup(),
                    requestHeader.getDestGroup(), requestHeader.getTopic());
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }
}
