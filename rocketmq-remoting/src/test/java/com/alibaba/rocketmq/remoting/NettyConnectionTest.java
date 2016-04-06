package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.alibaba.rocketmq.remoting.exception.RemotingConnectException;
import com.alibaba.rocketmq.remoting.exception.RemotingSendRequestException;
import com.alibaba.rocketmq.remoting.exception.RemotingTimeoutException;
import com.alibaba.rocketmq.remoting.netty.NettyClientConfig;
import com.alibaba.rocketmq.remoting.netty.NettyRemotingClient;
import org.junit.Test;


/**
 * 连接超时测试
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 2013-7-6
 */
public class NettyConnectionTest {
    public static RemotingClient createRemotingClient() {
        NettyClientConfig config = new NettyClientConfig();
        config.setClientChannelMaxIdleTimeSeconds(15);
        RemotingClient client = new NettyRemotingClient(config);
        client.start();
        return client;
    }


    @Test
    public void test_connect_timeout() throws InterruptedException, RemotingConnectException,
            RemotingSendRequestException, RemotingTimeoutException {
        RemotingClient client = createRemotingClient();

        for (int i = 0; i < 100; i++) {
            try {
                MessageCommand request = CommandUtil.createRequestCommand(0);
                MessageCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        client.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }
}
