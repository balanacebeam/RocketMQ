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
package com.alibaba.rocketmq.namesrv.processor;

import com.alibaba.rocketmq.common.MQVersion;
import com.alibaba.rocketmq.common.MQVersion.Version;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.help.FAQUrl;
import com.alibaba.rocketmq.common.namesrv.NamesrvUtil;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.RequestCode;
import com.alibaba.rocketmq.common.protocol.ResponseCode;
import com.alibaba.rocketmq.common.protocol.body.RegisterBrokerBody;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.protobuf.BrokerHeader.GetTopicsByClusterRequestHeader;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.common.protocol.protobuf.NamesrvHeader.*;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.namesrv.NamesrvController;
import com.alibaba.rocketmq.remoting.common.RemotingHelper;
import com.alibaba.rocketmq.remoting.exception.MessageCommandException;
import com.alibaba.rocketmq.remoting.netty.NettyRequestProcessor;
import io.netty.channel.ChannelHandlerContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;


/**
 * Name Server网络请求处理
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-5
 */
public class DefaultRequestProcessor implements NettyRequestProcessor {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);

    private final NamesrvController namesrvController;


    public DefaultRequestProcessor(NamesrvController namesrvController) {
        this.namesrvController = namesrvController;
    }


    @Override
    public MessageCommand processRequest(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (log.isDebugEnabled()) {
            log.debug("receive request, {} {} {}",//
                    request.getCode(), //
                    RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                    request);
        }

        switch (request.getCode()) {
            case RequestCode.PUT_KV_CONFIG:
                return this.putKVConfig(ctx, request);
            case RequestCode.GET_KV_CONFIG:
                return this.getKVConfig(ctx, request);
            case RequestCode.DELETE_KV_CONFIG:
                return this.deleteKVConfig(ctx, request);
            case RequestCode.REGISTER_BROKER:
                Version brokerVersion = MQVersion.value2Version(request.getVersion());
                // 新版本Broker，支持Filter Server
                if (brokerVersion.ordinal() >= MQVersion.Version.V3_0_11.ordinal()) {
                    return this.registerBrokerWithFilterServer(ctx, request);
                }
                // 低版本Broker，不支持Filter Server
                else {
                    return this.registerBroker(ctx, request);
                }
            case RequestCode.UNREGISTER_BROKER:
                return this.unregisterBroker(ctx, request);
            case RequestCode.GET_ROUTEINTO_BY_TOPIC:
                return this.getRouteInfoByTopic(ctx, request);
            case RequestCode.GET_BROKER_CLUSTER_INFO:
                return this.getBrokerClusterInfo(ctx, request);
            case RequestCode.WIPE_WRITE_PERM_OF_BROKER:
                return this.wipeWritePermOfBroker(ctx, request);
            case RequestCode.GET_ALL_TOPIC_LIST_FROM_NAMESERVER:
                return getAllTopicListFromNameserver(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_NAMESRV:
                return deleteTopicInNamesrv(ctx, request);
            case RequestCode.GET_KV_CONFIG_BY_VALUE:
                return getKVConfigByValue(ctx, request);
            case RequestCode.DELETE_KV_CONFIG_BY_VALUE:
                return deleteKVConfigByValue(ctx, request);
            case RequestCode.GET_KVLIST_BY_NAMESPACE:
                return this.getKVListByNamespace(ctx, request);
            case RequestCode.GET_TOPICS_BY_CLUSTER:
                return this.getTopicsByCluster(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_NS:
                return this.getSystemTopicListFromNs(ctx, request);
            case RequestCode.GET_UNIT_TOPIC_LIST:
                return this.getUnitTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_TOPIC_LIST:
                return this.getHasUnitSubTopicList(ctx, request);
            case RequestCode.GET_HAS_UNIT_SUB_UNUNIT_TOPIC_LIST:
                return this.getHasUnitSubUnUnitTopicList(ctx, request);
            default:
                break;
        }
        return null;
    }


    public MessageCommand registerBrokerWithFilterServer(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasRegisterBrokerRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final RegisterBrokerRequestHeader requestHeader = request.getRegisterBrokerRequestHeader();

        RegisterBrokerBody registerBrokerBody = new RegisterBrokerBody();

        if (request.hasBody()) {
            registerBrokerBody = RegisterBrokerBody.decode(request.getBody().toByteArray(), RegisterBrokerBody.class);
        } else {
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion()
                    .setCounter(new AtomicLong(0));
            registerBrokerBody.getTopicConfigSerializeWrapper().getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(//
                requestHeader.getClusterName(), // 1
                requestHeader.getBrokerAddr(), // 2
                requestHeader.getBrokerName(), // 3
                requestHeader.getBrokerId(), // 4
                requestHeader.getHaServerAddr(),// 5
                registerBrokerBody.getTopicConfigSerializeWrapper(), // 6
                registerBrokerBody.getFilterServerList(),//
                ctx.channel()// 7
        );

        // 获取顺序消息 topic 列表
        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(
                        NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);

        RegisterBrokerResponseHeader.Builder headerBuilder = RegisterBrokerResponseHeader.newBuilder();
        if (StringUtils.isNotEmpty(result.getMasterAddr())) {
            headerBuilder.setMasterAddr(result.getMasterAddr());
        }
        if (StringUtils.isNotEmpty(result.getHaServerAddr())) {
            headerBuilder.setHaServerAddr(result.getHaServerAddr());
        }

        return CommandUtil.createResponseBuilder(request.getOpaque(), jsonValue)
                .setRegisterBrokerResponseHeader(headerBuilder)
                .build();
    }


    /**
     * 获取一个Namespace下的所有kv
     */
    private MessageCommand getKVListByNamespace(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasGetKVListByNamespaceRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final GetKVListByNamespaceRequestHeader requestHeader = request.getGetKVListByNamespaceRequestHeader();

        byte[] jsonValue = this.namesrvController.getKvConfigManager().getKVListByNamespace(//
                requestHeader.getKeyspace());

        if (jsonValue == null) {
            return CommandUtil.createResponseCommand(ResponseCode.QUERY_NOT_FOUND,
                    request.getOpaque(),
                    "No config item, Namespace: " + requestHeader.getKeyspace());
        }

        return CommandUtil.createResponseBuilder(request.getOpaque(), jsonValue)
                .build();
    }


    private MessageCommand deleteTopicInNamesrv(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasDeleteTopicInNamesrvRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final DeleteTopicInNamesrvRequestHeader requestHeader = request.getDeleteTopicInNamesrvRequestHeader();

        this.namesrvController.getRouteInfoManager().deleteTopic(requestHeader.getTopic());

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    /**
     * 获取全部Topic列表
     *
     * @param ctx
     * @param request
     * @return
     */
    private MessageCommand getAllTopicListFromNameserver(ChannelHandlerContext ctx, MessageCommand request) {
        byte[] body = this.namesrvController.getRouteInfoManager().getAllTopicList();

        return CommandUtil.createResponseBuilder(request.getOpaque(), body)
                .build();
    }


    private MessageCommand wipeWritePermOfBroker(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasWipeWritePermOfBrokerRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final WipeWritePermOfBrokerRequestHeader requestHeader = request.getWipeWritePermOfBrokerRequestHeader();

        int wipeTopicCnt = this.namesrvController.getRouteInfoManager().wipeWritePermOfBrokerByLock(
                requestHeader.getBrokerName());

        log.info("wipe write perm of broker[{}], client: {}, {}", //
                requestHeader.getBrokerName(), //
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()), //
                wipeTopicCnt);

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setWipeWritePermOfBrokerResponseHeader(WipeWritePermOfBrokerResponseHeader.newBuilder()
                        .setWipeTopicCount(wipeTopicCnt))
                .build();
    }


    private MessageCommand getBrokerClusterInfo(ChannelHandlerContext ctx, MessageCommand request) {
        byte[] content = this.namesrvController.getRouteInfoManager().getAllClusterInfo();

        return CommandUtil.createResponseBuilder(request.getOpaque(), content)
                .build();
    }


    public MessageCommand getRouteInfoByTopic(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasGetRouteInfoRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final GetRouteInfoRequestHeader requestHeader = request.getGetRouteInfoRequestHeader();

        TopicRouteData topicRouteData = this.namesrvController.getRouteInfoManager().pickupTopicRouteData(requestHeader.getTopic());

        if (topicRouteData == null) {
            return CommandUtil.createResponseCommand(ResponseCode.TOPIC_NOT_EXIST,
                    request.getOpaque(),
                    "No topic route info in name server for the topic: " + requestHeader.getTopic()
                            + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
        }

        String orderTopicConf = this.namesrvController.getKvConfigManager().getKVConfig(
                NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG, requestHeader.getTopic());
        topicRouteData.setOrderTopicConf(orderTopicConf);

        byte[] content = topicRouteData.encode();

        return CommandUtil.createResponseBuilder(request.getOpaque(), content)
                .build();
    }


    public MessageCommand putKVConfig(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasPutKVConfigRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final PutKVConfigRequestHeader requestHeader = request.getPutKVConfigRequestHeader();

        this.namesrvController.getKvConfigManager().putKVConfig(//
                requestHeader.getKeyspace(),//
                requestHeader.getKey(),//
                requestHeader.getValue()//
        );

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    public MessageCommand getKVConfig(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasGetKVConfigRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final GetKVConfigRequestHeader requestHeader = request.getGetKVConfigRequestHeader();

        String value = this.namesrvController.getKvConfigManager().getKVConfig(//
                requestHeader.getKeyspace(),//
                requestHeader.getKey()//
        );

        if (value == null) {
            return CommandUtil.createResponseCommand(ResponseCode.QUERY_NOT_FOUND,
                    request.getOpaque(),
                    "No config item, Namespace: " + requestHeader.getKeyspace() + " Key: "
                            + requestHeader.getKey());
        }

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setGetKVConfigResponseHeader(GetKVConfigResponseHeader.newBuilder()
                        .setValue(value))
                .build();
    }


    public MessageCommand deleteKVConfig(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasDeleteKVConfigRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final DeleteKVConfigRequestHeader requestHeader = request.getDeleteKVConfigRequestHeader();

        this.namesrvController.getKvConfigManager().deleteKVConfig(//
                requestHeader.getKeyspace(),//
                requestHeader.getKey()//
        );

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    public MessageCommand registerBroker(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasRegisterBrokerRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final RegisterBrokerRequestHeader requestHeader = request.getRegisterBrokerRequestHeader();

        TopicConfigSerializeWrapper topicConfigWrapper = null;
        if (request.hasBody()) {
            topicConfigWrapper = TopicConfigSerializeWrapper.decode(request.getBody().toByteArray(), TopicConfigSerializeWrapper.class);
        } else {
            topicConfigWrapper = new TopicConfigSerializeWrapper();
            topicConfigWrapper.getDataVersion().setCounter(new AtomicLong(0));
            topicConfigWrapper.getDataVersion().setTimestatmp(0);
        }

        RegisterBrokerResult result = this.namesrvController.getRouteInfoManager().registerBroker(//
                requestHeader.getClusterName(), // 1
                requestHeader.getBrokerAddr(), // 2
                requestHeader.getBrokerName(), // 3
                requestHeader.getBrokerId(), // 4
                requestHeader.getHaServerAddr(),// 5
                topicConfigWrapper, // 6
                null,//
                ctx.channel()// 7
        );

        // 获取顺序消息 topic 列表
        byte[] jsonValue =
                this.namesrvController.getKvConfigManager().getKVListByNamespace(
                        NamesrvUtil.NAMESPACE_ORDER_TOPIC_CONFIG);

        RegisterBrokerResponseHeader.Builder requestBuilder = RegisterBrokerResponseHeader.newBuilder();
        if (StringUtils.isNotEmpty(result.getHaServerAddr())) {
            requestBuilder.setHaServerAddr(result.getHaServerAddr());
        }
        if (StringUtils.isNotEmpty(result.getMasterAddr())) {
            requestBuilder.setMasterAddr(result.getMasterAddr());
        }

        return CommandUtil.createResponseBuilder(request.getOpaque(), jsonValue)
                .setRegisterBrokerResponseHeader(requestBuilder)
                .build();
    }


    public MessageCommand unregisterBroker(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasUnRegisterBrokerRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final UnRegisterBrokerRequestHeader requestHeader = request.getUnRegisterBrokerRequestHeader();

        this.namesrvController.getRouteInfoManager().unregisterBroker(//
                requestHeader.getClusterName(), // 1
                requestHeader.getBrokerAddr(), // 2
                requestHeader.getBrokerName(), // 3
                requestHeader.getBrokerId());

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    public MessageCommand getKVConfigByValue(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasGetKVConfigRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final GetKVConfigRequestHeader requestHeader = request.getGetKVConfigRequestHeader();

        String value = this.namesrvController.getKvConfigManager().getKVConfigByValue(//
                requestHeader.getKeyspace(),//
                requestHeader.getKey()//
        );

        if (value == null) {
            return CommandUtil.createResponseCommand(ResponseCode.QUERY_NOT_FOUND,
                    request.getOpaque(),
                    "No config item, Namespace: " + requestHeader.getKeyspace() + " Key: "
                            + requestHeader.getKey());
        }

        return CommandUtil.createResponseBuilder(request.getOpaque())
                .setGetKVConfigResponseHeader(GetKVConfigResponseHeader.newBuilder()
                        .setValue(value))
                .build();
    }


    public MessageCommand deleteKVConfigByValue(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasDeleteKVConfigRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final DeleteKVConfigRequestHeader requestHeader = request.getDeleteKVConfigRequestHeader();

        this.namesrvController.getKvConfigManager().deleteKVConfigByValue(//
                requestHeader.getKeyspace(),//
                requestHeader.getKey()//
        );

        return CommandUtil.createResponseCommandSuccess(request.getOpaque());
    }


    /**
     * 获取指定集群下的全部Topic列表
     *
     * @param ctx
     * @param request
     * @return
     */
    private MessageCommand getTopicsByCluster(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        if (!request.hasGetTopicsByClusterRequestHeader()) {
            return CommandUtil.createResponseCommandFail(request.getOpaque());
        }

        final GetTopicsByClusterRequestHeader requestHeader = request.getGetTopicsByClusterRequestHeader();

        byte[] body = this.namesrvController.getRouteInfoManager().getTopicsByCluster(requestHeader.getCluster());

        return CommandUtil.createResponseCommand(body, request.getOpaque());
    }


    /**
     * 获取所有系统内置 Topic 列表
     *
     * @param ctx
     * @param request
     * @return
     * @throws MessageCommandException
     */
    private MessageCommand getSystemTopicListFromNs(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        byte[] body = this.namesrvController.getRouteInfoManager().getSystemTopicList();

        return CommandUtil.createResponseCommand(body, request.getOpaque());
    }


    /**
     * 获取单元化逻辑 Topic 列表
     *
     * @param ctx
     * @param request
     * @return
     * @throws MessageCommandException
     */
    private MessageCommand getUnitTopicList(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        byte[] body = this.namesrvController.getRouteInfoManager().getUnitTopics();

        return CommandUtil.createResponseCommand(body, request.getOpaque());
    }


    /**
     * 获取含有单元化订阅组的 Topic 列表
     *
     * @param ctx
     * @param request
     * @return
     * @throws MessageCommandException
     */
    private MessageCommand getHasUnitSubTopicList(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubTopicList();

        return CommandUtil.createResponseCommand(body, request.getOpaque());
    }


    /**
     * 获取含有单元化订阅组的非单元化 Topic 列表
     *
     * @param ctx
     * @param request
     * @return
     * @throws MessageCommandException
     */
    private MessageCommand getHasUnitSubUnUnitTopicList(ChannelHandlerContext ctx, MessageCommand request)
            throws MessageCommandException {
        byte[] body = this.namesrvController.getRouteInfoManager().getHasUnitSubUnUnitTopicList();

        return CommandUtil.createResponseCommand(body, request.getOpaque());
    }
}
