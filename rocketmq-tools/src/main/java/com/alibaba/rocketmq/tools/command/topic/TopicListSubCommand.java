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
package com.alibaba.rocketmq.tools.command.topic;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.UtilAll;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.GroupList;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.remoting.RPCHook;
import com.alibaba.rocketmq.remoting.exception.RemotingException;
import com.alibaba.rocketmq.tools.admin.DefaultMQAdminExt;
import com.alibaba.rocketmq.tools.command.SubCommand;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;


/**
 * 查看Topic统计信息，包括offset、最后更新时间
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-8-3
 */
public class TopicListSubCommand implements SubCommand {

    @Override
    public String commandName() {
        return "topicList";
    }


    @Override
    public String commandDesc() {
        return "Fetch all topic list from name server";
    }


    @Override
    public Options buildCommandlineOptions(Options options) {
        Option opt = new Option("c", "clusterModel", false, "clusterModel");
        opt.setRequired(false);
        options.addOption(opt);

        return options;
    }


    private String findTopicBelongToWhichCluster(final String topic, final ClusterInfo clusterInfo,
                                                 final DefaultMQAdminExt defaultMQAdminExt) throws RemotingException, MQClientException,
            InterruptedException {
        TopicRouteData topicRouteData = defaultMQAdminExt.examineTopicRouteInfo(topic);

        BrokerData brokerData = topicRouteData.getBrokerDatas().get(0);

        String brokerName = brokerData.getBrokerName();

        Iterator<Entry<String, Set<String>>> it = clusterInfo.getClusterAddrTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<String>> next = it.next();
            if (next.getValue().contains(brokerName)) {
                return next.getKey();
            }
        }

        return null;
    }


    @Override
    public void execute(final CommandLine commandLine, final Options options, RPCHook rpcHook) {
        DefaultMQAdminExt defaultMQAdminExt = new DefaultMQAdminExt(rpcHook);

        defaultMQAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));

        try {
            defaultMQAdminExt.start();

            if (commandLine.hasOption('c')) {
                ClusterInfo clusterInfo = defaultMQAdminExt.examineBrokerClusterInfo();

                System.out.printf("%-20s  %-48s  %-48s\n",//
                        "#Cluster Name",//
                        "#Topic",//
                        "#Consumer Group"//
                );

                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    if (topic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)
                            || topic.startsWith(MixAll.DLQ_GROUP_TOPIC_PREFIX)) {
                        continue;
                    }

                    String clusterName = "";
                    GroupList groupList = new GroupList();

                    try {
                        clusterName =
                                this.findTopicBelongToWhichCluster(topic, clusterInfo, defaultMQAdminExt);
                        groupList = defaultMQAdminExt.queryTopicConsumeByWho(topic);
                    } catch (Exception e) {
                    }

                    if (null == groupList || groupList.getGroupList().isEmpty()) {
                        groupList = new GroupList();
                        groupList.getGroupList().add("");
                    }

                    for (String group : groupList.getGroupList()) {
                        System.out.printf("%-20s  %-48s  %-48s\n",//
                                UtilAll.frontStringAtLeast(clusterName, 20),//
                                UtilAll.frontStringAtLeast(topic, 48),//
                                UtilAll.frontStringAtLeast(group, 48)//
                        );
                    }
                }
            } else {
                TopicList topicList = defaultMQAdminExt.fetchAllTopicList();
                for (String topic : topicList.getTopicList()) {
                    System.out.println(topic);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            defaultMQAdminExt.shutdown();
        }
    }
}
