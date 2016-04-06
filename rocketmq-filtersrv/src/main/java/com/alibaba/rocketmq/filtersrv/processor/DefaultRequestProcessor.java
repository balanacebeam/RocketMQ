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
package com.alibaba.rocketmq.filtersrv.processor;

import com.alibaba.rocketmq.client.consumer.DefaultMQPullConsumer;
import com.alibaba.rocketmq.client.consumer.PullCallback;
import com.alibaba.rocketmq.client.consumer.PullResult;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.message.MessageDecoder;
import com.alibaba.rocketmq.common.message.MessageExt;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.PullMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.PullMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.protocol.protobuf.FiltersrvHeader.RegisterMessageFilterClassRequestHeader;
import com.alibaba.rocketmq.common.sysflag.MessageSysFlag;
import com.alibaba.rocketmq.filtersrv.FiltersrvController;
import com.alibaba.rocketmq.filtersrv.filter.FilterClassInfo;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.store.CommitLog;
import com.google.protobuf.ByteString;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;


/**
 * Filter Server网络请求处理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2014-4-20
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.FiltersrvLoggerName);

    private final FiltersrvController filtersrvController;


    public DefaultRequestProcessor(FiltersrvController filtersrvController) {
        this.filtersrvController = filtersrvController;
    }


    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws Exception {
        if (log.isDebugEnabled()) {
            log.debug("receive request, {} {} {}",//
                    request.getCode(), //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.REGISTER_MESSAGE_FILTER_CLASS:
                return registerMessageFilterClass(ctx, request);
            case RequestCode.PULL_MESSAGE:
                return pullMessageForward(ctx, request);
        }

        return null;
    }


    private ByteBuffer messageToByteBuffer(final MessageExt msg) throws IOException {
        int sysFlag = MessageSysFlag.clearCompressedFlag(msg.getSysFlag());
        if (msg.getBody() != null) {
            if (msg.getBody().length >= this.filtersrvController.getFiltersrvConfig()
                    .getCompressMsgBodyOverHowmuch()) {
                byte[] data =
                        UtilAll.compress(msg.getBody(), this.filtersrvController.getFiltersrvConfig()
                                .getZipCompressLevel());
                if (data != null) {
                    msg.setBody(data);
                    sysFlag |= MessageSysFlag.CompressedFlag;
                }
            }
        }

        final int bodyLength = msg.getBody() != null ? msg.getBody().length : 0;
        byte[] topicData = msg.getTopic().getBytes(MixAll.DEFAULT_CHARSET);
        final int topicLength = topicData.length;
        String properties = MessageDecoder.messageProperties2String(msg.getProperties());
        byte[] propertiesData = properties.getBytes(MixAll.DEFAULT_CHARSET);
        final int propertiesLength = propertiesData.length;
        final int msgLen = 4 // 1 TOTALSIZE
                + 4 // 2 MAGICCODE
                + 4 // 3 BODYCRC
                + 4 // 4 QUEUEID
                + 4 // 5 FLAG
                + 8 // 6 QUEUEOFFSET
                + 8 // 7 PHYSICALOFFSET
                + 4 // 8 SYSFLAG
                + 8 // 9 BORNTIMESTAMP
                + 8 // 10 BORNHOST
                + 8 // 11 STORETIMESTAMP
                + 8 // 12 STOREHOSTADDRESS
                + 4 // 13 RECONSUMETIMES
                + 8 // 14 Prepared Transaction Offset
                + 4 + bodyLength // 14 BODY
                + 1 + topicLength // 15 TOPIC
                + 2 + propertiesLength // 16 propertiesLength
                + 0;

        ByteBuffer msgStoreItemMemory = ByteBuffer.allocate(msgLen);

        final MessageExt msgInner = msg;

        // 1 TOTALSIZE
        msgStoreItemMemory.putInt(msgLen);
        // 2 MAGICCODE
        msgStoreItemMemory.putInt(CommitLog.MessageMagicCode);
        // 3 BODYCRC
        msgStoreItemMemory.putInt(UtilAll.crc32(msgInner.getBody()));
        // 4 QUEUEID
        msgStoreItemMemory.putInt(msgInner.getQueueId());
        // 5 FLAG
        msgStoreItemMemory.putInt(msgInner.getFlag());
        // 6 QUEUEOFFSET
        msgStoreItemMemory.putLong(msgInner.getQueueOffset());
        // 7 PHYSICALOFFSET
        msgStoreItemMemory.putLong(msgInner.getCommitLogOffset());
        // 8 SYSFLAG
        msgStoreItemMemory.putInt(sysFlag);
        // 9 BORNTIMESTAMP
        msgStoreItemMemory.putLong(msgInner.getBornTimestamp());
        // 10 BORNHOST
        msgStoreItemMemory.put(msgInner.getBornHostBytes());
        // 11 STORETIMESTAMP
        msgStoreItemMemory.putLong(msgInner.getStoreTimestamp());
        // 12 STOREHOSTADDRESS
        msgStoreItemMemory.put(msgInner.getStoreHostBytes());
        // 13 RECONSUMETIMES
        msgStoreItemMemory.putInt(msgInner.getReconsumeTimes());
        // 14 Prepared Transaction Offset
        msgStoreItemMemory.putLong(msgInner.getPreparedTransactionOffset());
        // 15 BODY
        msgStoreItemMemory.putInt(bodyLength);
        if (bodyLength > 0)
            msgStoreItemMemory.put(msgInner.getBody());
        // 16 TOPIC
        msgStoreItemMemory.put((byte) topicLength);
        msgStoreItemMemory.put(topicData);
        // 17 PROPERTIES
        msgStoreItemMemory.putShort((short) propertiesLength);
        if (propertiesLength > 0)
            msgStoreItemMemory.put(propertiesData);

        return msgStoreItemMemory;
    }


    private void returnResponse(final String group, final String topic, ChannelHandlerContext ctx,
                                final MessageCommand.Builder response, final List<MessageExt> msgList) {
        if (null != msgList) {
            ByteBuffer[] msgBufferList = new ByteBuffer[msgList.size()];
            int bodyTotalSize = 0;
            for (int i = 0; i < msgList.size(); i++) {
                try {
                    msgBufferList[i] = messageToByteBuffer(msgList.get(i));
                    bodyTotalSize += msgBufferList[i].capacity();
                } catch (Exception e) {
                    log.error("messageToByteBuffer UnsupportedEncodingException", e);
                }
            }

            ByteBuffer body = ByteBuffer.allocate(bodyTotalSize);
            for (ByteBuffer bb : msgBufferList) {
                bb.flip();
                body.put(bb);
            }

            if (body.hasRemaining()) {
                response.setBody(ByteString.copyFrom(body.array()));
            }

            // 统计
            this.filtersrvController.getFilterServerStatsManager().incGroupGetNums(group, topic,
                    msgList.size());

            this.filtersrvController.getFilterServerStatsManager().incGroupGetSize(group, topic,
                    bodyTotalSize);
        }

        try {
            ctx.writeAndFlush(response.build()).addListener(new ChannelFutureListener() {
                @Override
                public void operationComplete(ChannelFuture future) throws Exception {
                    if (!future.isSuccess()) {
                        log.error("FilterServer response to " + future.channel().remoteAddress() + " failed",
                                future.cause());
                        log.error(response.toString());
                    }
                }
            });
        } catch (Throwable e) {
            log.error("FilterServer process request over, but response failed", e);
            log.error(response.toString());
        }
    }


    private MessageCommand pullMessageForward(final ChannelHandlerContext ctx, final MessageCommand request)
            throws Exception {
        final PullMessageRequestHeader requestHeader = request.getPullMessageRequestHeader();

        DefaultMQPullConsumer pullConsumer = this.filtersrvController.getDefaultMQPullConsumer();
        final FilterClassInfo findFilterClass =
                this.filtersrvController.getFilterClassManager().findFilterClass(
                        requestHeader.getConsumerGroup(), requestHeader.getTopic());
        if (null == findFilterClass) {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    request.getOpaque(),
                    "Find Filter class failed, not registered");
        }

        if (null == findFilterClass.getMessageFilter()) {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    request.getOpaque(),
                    "Find Filter class failed, registered but no class");
        }

        final PullMessageResponseHeader.Builder responseHeaderBuilder = PullMessageResponseHeader.newBuilder()
                .setSuggestWhichBrokerId(MixAll.MASTER_ID);

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque())
                .setPullMessageResponseHeader(responseHeaderBuilder);

        // 构造从Broker拉消息的参数
        MessageQueue mq = new MessageQueue();
        mq.setTopic(requestHeader.getTopic());
        mq.setQueueId(requestHeader.getQueueId());
        mq.setBrokerName(this.filtersrvController.getBrokerName());
        long offset = requestHeader.getQueueOffset();
        int maxNums = requestHeader.getMaxMsgNums();

        final PullCallback pullCallback = new PullCallback() {

            @Override
            public void onSuccess(PullResult pullResult) {
                responseHeaderBuilder.setMaxOffset(pullResult.getMaxOffset());
                responseHeaderBuilder.setMinOffset(pullResult.getMinOffset());
                responseHeaderBuilder.setNextBeginOffset(pullResult.getNextBeginOffset());

                switch (pullResult.getPullStatus()) {
                    case FOUND:
                        responseBuilder.setCode(ResponseCode.SUCCESS);

                        List<MessageExt> msgListOK = new ArrayList<MessageExt>();
                        try {
                            for (MessageExt msg : pullResult.getMsgFoundList()) {
                                boolean match = findFilterClass.getMessageFilter().match(msg);
                                if (match) {
                                    msgListOK.add(msg);
                                }
                            }

                            // 有消息返回
                            if (!msgListOK.isEmpty()) {
                                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx,
                                        responseBuilder, msgListOK);
                                return;
                            }
                            // 全部都被过滤掉了
                            else {
                                responseBuilder.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                            }
                        }
                        // 只要抛异常，就终止过滤，并返回客户端异常
                        catch (Throwable e) {
                            final String error =
                                    String.format("do Message Filter Exception, ConsumerGroup: %s Topic: %s ",
                                            requestHeader.getConsumerGroup(), requestHeader.getTopic());
                            log.error(error, e);

                            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                            responseBuilder.setRemark(error + UtilAll.exceptionSimpleDesc(e));
                            returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx,
                                    responseBuilder, null);
                            return;
                        }

                        break;
                    case NO_MATCHED_MSG:
                        responseBuilder.setCode(ResponseCode.PULL_RETRY_IMMEDIATELY);
                        break;
                    case NO_NEW_MSG:
                        responseBuilder.setCode(ResponseCode.PULL_NOT_FOUND);
                        break;
                    case OFFSET_ILLEGAL:
                        responseBuilder.setCode(ResponseCode.PULL_OFFSET_MOVED);
                        break;
                    default:
                        break;
                }

                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, responseBuilder,
                        null);
            }


            @Override
            public void onException(Throwable e) {
                responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
                responseBuilder.setRemark("Pull Callback Exception, " + UtilAll.exceptionSimpleDesc(e));
                returnResponse(requestHeader.getConsumerGroup(), requestHeader.getTopic(), ctx, responseBuilder,
                        null);
                return;
            }
        };

        pullConsumer.pullBlockIfNotFound(mq, null, offset, maxNums, pullCallback);

        return null;
    }


    private MessageCommand registerMessageFilterClass(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final RegisterMessageFilterClassRequestHeader requestHeader = request.getRegisterMessageFilterClassRequestHeader();

        try {
            boolean ok =
                    this.filtersrvController.getFilterClassManager().registerFilterClass(
                            requestHeader.getConsumerGroup(),//
                            requestHeader.getTopic(),//
                            requestHeader.getClassName(),//
                            requestHeader.getClassCRC(), //
                            request.getBody().toByteArray());// Body传输的是Java Source，必须UTF-8编码
            if (!ok) {
                throw new Exception("registerFilterClass error");
            }
        } catch (Exception e) {
            return CommandUtil.createResponseCommand(ResponseCode.SYSTEM_ERROR,
                    request.getOpaque(),
                    UtilAll.exceptionSimpleDesc(e));
        }

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }
}
