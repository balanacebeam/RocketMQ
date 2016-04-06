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
import com.alibaba.rocketmq.broker.pagecache.OneMessageTransfer;
import com.alibaba.rocketmq.broker.pagecache.QueryMessageTransfer;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.QueryMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.QueryMessageResponseHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.ViewMessageRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import com.alibaba.rocketmq.store.QueryMessageResult;
import com.alibaba.rocketmq.store.SelectMapedBufferResult;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.FileRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


/**
 * 查询消息请求处理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-26
 */
public class QueryMessageProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BrokerLoggerName);

    private final BrokerController brokerController;


    public QueryMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }


    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        switch (request.getCode()) {
            case RequestCode.QUERY_MESSAGE:
                return this.queryMessage(ctx, request);
            case RequestCode.VIEW_MESSAGE_BY_ID:
                return this.viewMessageById(ctx, request);
            default:
                break;
        }

        return null;
    }


    public MessageCommand queryMessage(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final QueryMessageRequestHeader requestHeader = request.getQueryMessageRequestHeader();

        final QueryMessageResult queryMessageResult =
                this.brokerController.getMessageStore().queryMessage(requestHeader.getTopic(),
                        requestHeader.getKey(), requestHeader.getMaxNum(), requestHeader.getBeginTimestamp(),
                        requestHeader.getEndTimestamp());
        assert queryMessageResult != null;

        final MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque())
                .setQueryMessageResponseHeader(QueryMessageResponseHeader.newBuilder()
                .setIndexLastUpdatePhyOffset(queryMessageResult.getIndexLastUpdatePhyoffset())
                .setIndexLastUpdateTimestamp(queryMessageResult.getIndexLastUpdateTimestamp()));
        // 说明找到消息
        if (queryMessageResult.getBufferTotalSize() > 0) {
            responseBuilder.setCode(ResponseCode.SUCCESS);

            try {
                FileRegion fileRegion =
                        new QueryMessageTransfer(ByteBuffer.wrap(responseBuilder.build().toByteArray()), queryMessageResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        queryMessageResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer query message by pagecache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                queryMessageResult.release();
            }

            return null;
        }

        responseBuilder.setCode(ResponseCode.QUERY_NOT_FOUND);
        responseBuilder.setRemark("can not find message, maybe time range not correct");

        return responseBuilder.build();
    }


    public MessageCommand viewMessageById(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        final ViewMessageRequestHeader requestHeader = request.getViewMessageRequestHeader();

        final SelectMapedBufferResult selectMapedBufferResult =
                this.brokerController.getMessageStore().selectOneMessageByOffset(requestHeader.getOffset());

        MessageCommand.Builder responseBuilder = CommandUtil.createResponseBuilder(request.getOpaque());
        if (selectMapedBufferResult != null) {
            responseBuilder.setCode(ResponseCode.SUCCESS);

            try {
                FileRegion fileRegion =
                        new OneMessageTransfer(ByteBuffer.wrap(responseBuilder.build().toByteArray()),
                                selectMapedBufferResult);
                ctx.channel().writeAndFlush(fileRegion).addListener(new ChannelFutureListener() {
                    @Override
                    public void operationComplete(ChannelFuture future) throws Exception {
                        selectMapedBufferResult.release();
                        if (!future.isSuccess()) {
                            log.error("transfer one message by pagecache failed, ", future.cause());
                        }
                    }
                });
            } catch (Throwable e) {
                log.error("", e);
                selectMapedBufferResult.release();
            }

            return null;
        } else {
            responseBuilder.setCode(ResponseCode.SYSTEM_ERROR);
            responseBuilder.setRemark("can not find message by the offset, " + requestHeader.getOffset());
        }

        return responseBuilder.build();
    }
}
