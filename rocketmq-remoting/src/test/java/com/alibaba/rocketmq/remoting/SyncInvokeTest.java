/**
 * $Id: SyncInvokeTest.java 1831 2013-05-16 01:39:51Z shijia.wxr $
 */
package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.common.protocol.CommandUtil;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import org.junit.Test;

import static org.junit.Assert.assertTrue;


/**
 * @author shijia.wxr<vintage.wang@gmail.com>
 */
public class SyncInvokeTest {
    @Test
    public void test_RPC_Sync() throws Exception {
        RemotingServer server = NettyRPCTest.createRemotingServer();
        RemotingClient client = NettyRPCTest.createRemotingClient();

        for (int i = 0; i < 100; i++) {
            try {
                MessageCommand request = CommandUtil.createRequestCommand(0);
                MessageCommand response = client.invokeSync("localhost:8888", request, 1000 * 3);
                System.out.println(i + "\t" + "invoke result = " + response);
                assertTrue(response != null);
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }
        }

        client.shutdown();
        server.shutdown();
        System.out.println("-----------------------------------------------------------------");
    }
}
