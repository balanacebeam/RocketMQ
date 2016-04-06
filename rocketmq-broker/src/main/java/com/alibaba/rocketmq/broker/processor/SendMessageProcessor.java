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
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.ConsumeMessageHook;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageContext;
import com.alibaba.rocketmq.broker.mqtrace.SendMessageHook;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.message.MessageAccessor;
import com.alibaba.rocketmq.common.message.MessageConst;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.ConsumerSendMsgBackRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.SendMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.SendMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.TopicFilterType;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.subscription.SubscriptionGroupConfig;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.store.MessageExtBrokerInner;
import com.alibaba.rocketmq.store.PutMessageResult;
import com.alibaba.rocketmq.store.config.StorePathConfigHelper;
import io.netty.channel.ChannelHandlerContext;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * 处理客户端发送消息的请求
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {


    /**
     * 发送每条消息会回调
     */
    private List<SendMessageHook> sendMessageHookList;
    /**
     * 消费每条消息会回调
     */
    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request) throws MessageCommandException {
        SendMessageContext mqtraceContext = null;
        switch (request.getCode()) {
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.consumerSendMsgBack(ctx, request);
            default:
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return null;
                }
                // 消息轨迹：记录到达 broker 的消息
                mqtraceContext = buildMsgContext(ctx, requestHeader);
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);
                final MessageCommand response = this.sendMessage(ctx, request, mqtraceContext, requestHeader);
                // 消息轨迹：记录发送成功的消息
                this.executeSendMessageHookAfter(response, mqtraceContext);
                return response;
        }
    }

    private MessageCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final MessageCommand request)
            throws MessageCommandException {
        final ConsumerSendMsgBackRequestHeader requestHeader = request.getConsumerSendMsgBackRequestHeader();

        // 消息轨迹：记录消费失败的消息
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {
            // 执行hook
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getGroup());
            context.setTopic(requestHeader.getOriginTopic());
            context.setClientHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
            context.setSuccess(false);
            context.setStatus(ConsumeConcurrentlyStatus.RECONSUME_LATER.toString());

            Map<String, Long> messageIds = new HashMap<String, Long>();
            messageIds.put(requestHeader.getOriginMsgId(), requestHeader.getOffset());
            context.setMessageIds(messageIds);
            this.executeConsumeMessageHookAfter(context);
        }

        // 确保订阅组存在
        SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(
                        requestHeader.getGroup());

        MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (null == subscriptionGroupConfig) {
            responseBuilder.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            responseBuilder.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                    + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));

            return responseBuilder.build();
        }

        // 检查Broker权限
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                    + "] sending message is forbidden");

            return responseBuilder.build();
        }

        // 如果重试队列数目为0，则直接丢弃消息
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            responseBuilder.setCode(ResponseCode.SUCCESS);

            return responseBuilder.build();
        }

        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        int queueIdInt =
                Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

        // 如果是单元化模式，则对 topic 进行设置
        int topicSysFlag = 0;
        if (requestHeader.getUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        // 检查topic是否存在
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(//
                        newTopic,//
                        subscriptionGroupConfig.getRetryQueueNums(), //
                        PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        if (null == topicConfig) {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("topic[" + newTopic + "] not exist");
            return responseBuilder.build();
        }

        // 检查topic权限
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            responseBuilder.setCode(ResponseCode.NO_PERMISSION);
            responseBuilder.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return responseBuilder.build();
        }

        // 查询消息，这里如果堆积消息过多，会访问磁盘
        // 另外如果频繁调用，是否会引起gc问题，需要关注 TODO
        MessageExt msgExt =
                this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return responseBuilder.build();
        }

        // 构造消息
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        msgExt.setWaitStoreMsgOK(false);

        // 客户端自动决定定时级别
        int delayLevel = requestHeader.getDelayLevel();

        // 死信消息处理
        if (msgExt.getReconsumeTimes() >= subscriptionGroupConfig.getRetryMaxTimes()//
                || delayLevel < 0) {
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            topicConfig =
                    this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                            newTopic, //
                            DLQ_NUMS_PER_GROUP,//
                            PermName.PERM_WRITE, 0 // 死信消息不需要同步，不需要较正。
                    );
            if (null == topicConfig) {
                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("topic[" + newTopic + "] not exist");
                return responseBuilder.build();
            }
        }
        // 继续重试
        else {
            if (0 == delayLevel) {
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        // 保存源生消息的 msgId
        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId()
                : originMsgId);

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    // 统计失败重试的Topic
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }

                    this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(),
                            backTopic);

                    responseBuilder.setCode(ResponseCode.SUCCESS);

                    return responseBuilder.build();
                default:
                    break;
            }

            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark(putMessageResult.getPutMessageStatus().name());
            return responseBuilder.build();
        }

        responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
        responseBuilder.setRemark("putMessageResult is null");

        return responseBuilder.build();
    }

    private String diskUtil() {
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        String storePathLogis =
                StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig()
                        .getStorePathRootDir());
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
                StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig()
                        .getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    private MessageCommand sendMessage(final ChannelHandlerContext ctx, //
                                       final MessageCommand request,//
                                       final SendMessageContext mqtraceContext,//
                                       final SendMessageRequestHeader requestHeader) throws MessageCommandException {

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());

        // 由于有直接返回的逻辑，所以必须要设置
        responseBuilder.setOpaque(request.getOpaque());

        if (log.isDebugEnabled()) {
            log.debug("receive SendMessage request command, " + request);
        }
        responseBuilder.setCode(-1);
        super.msgCheck(ctx, requestHeader, responseBuilder);
        if (responseBuilder.getCode() != -1) {
            return responseBuilder.build();
        }


        final byte[] body = request.getBody().toByteArray();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig =
                this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        // 随机指定一个队列
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        int sysFlag = requestHeader.getSysFlag();
        // 多标签过滤需要置位
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MultiTagsFlag;
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
                MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
                msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReConsumeTimes());

        // 检查事务消息
        if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
            String traFlag = msgInner.getProperty(MessageConst.PROPERTY_TRANSACTION_PREPARED);
            if (traFlag != null) {
                responseBuilder.setCode(ResponseCode.NO_PERMISSION);
                responseBuilder.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");

                return responseBuilder.build();
            }
        }

        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            boolean sendOK = false;

            switch (putMessageResult.getPutMessageStatus()) {
                // Success
                case PUT_OK:
                    sendOK = true;
                    responseBuilder.setCode(ResponseCode.SUCCESS);
                    break;
                case FLUSH_DISK_TIMEOUT:
                    responseBuilder.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                    sendOK = true;
                    break;
                case FLUSH_SLAVE_TIMEOUT:
                    responseBuilder.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                    sendOK = true;
                    break;
                case SLAVE_NOT_AVAILABLE:
                    responseBuilder.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                    sendOK = true;
                    break;

                // Failed
                case CREATE_MAPEDFILE_FAILED:
                    responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                    responseBuilder.setRemark("create maped file failed, please make sure OS and JDK both 64bit.");
                    break;
                case MESSAGE_ILLEGAL:
                    responseBuilder.setCode(ResponseCode.MESSAGE_ILLEGAL);
                    responseBuilder.setRemark("the message is illegal, maybe length not matched.");
                    break;
                case SERVICE_NOT_AVAILABLE:
                    responseBuilder.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                    responseBuilder.setRemark("service not available now, maybe disk full, " + diskUtil()
                            + ", maybe your broker machine memory too small.");
                    break;
                case UNKNOWN_ERROR:
                    responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                    responseBuilder.setRemark("UNKNOWN_ERROR");
                    break;
                default:
                    responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                    responseBuilder.setRemark("UNKNOWN_ERROR DEFAULT");
                    break;
            }

            if (sendOK) {
                // 统计
                this.brokerController.getBrokerStatsManager().incTopicPutNums(msgInner.getTopic());
                this.brokerController.getBrokerStatsManager().incTopicPutSize(msgInner.getTopic(),
                        putMessageResult.getAppendMessageResult().getWroteBytes());
                this.brokerController.getBrokerStatsManager().incBrokerPutNums();


                final SendMessageResponseHeader responseHeader = SendMessageResponseHeader.newBuilder()
                        .setMsgId(putMessageResult.getAppendMessageResult().getMsgId())
                        .setQueueId(queueIdInt)
                        .setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset())
                        .build();
                responseBuilder.setSendMessageResponseHeader(responseHeader);

                // 直接返回
                doResponse(ctx, request, responseBuilder.build());
                if (this.brokerController.getBrokerConfig().isLongPollingEnable()) {
                    this.brokerController.getPullRequestHoldService().notifyMessageArriving(
                            requestHeader.getTopic(), queueIdInt,
                            putMessageResult.getAppendMessageResult().getLogicsOffset() + 1);
                }

                // 消息轨迹：记录发送成功的消息
                if (hasSendMessageHook()) {
                    mqtraceContext.setMsgId(responseHeader.getMsgId());
                    mqtraceContext.setQueueId(responseHeader.getQueueId());
                    mqtraceContext.setQueueOffset(responseHeader.getQueueOffset());
                }
                return null;
            }
        } else {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("store putMessage return null");
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
}
