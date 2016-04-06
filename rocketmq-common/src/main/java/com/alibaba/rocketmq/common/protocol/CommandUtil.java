package com.alibaba.rocketmq.common.protocol;

import com.alibaba.rocketmq.common.protocol.protobuf.Command;
import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.ArrayUtils;

import java.util.concurrent.atomic.AtomicInteger;

import static com.alibaba.rocketmq.common.protocol.protobuf.Command.RpcType.REQUEST;
import static com.alibaba.rocketmq.common.protocol.protobuf.Command.RpcType.RESPONSE;

/**
 * Created by diwayou on 16-4-1.
 */
public class CommandUtil {

    private static volatile int ConfigVersion = -1;

    private static AtomicInteger RequestId = new AtomicInteger(0);

    public static String RemotingVersionKey = "rocketmq.remoting.version";

    public static MessageCommand createResponseCommand(int code, int opaque, String remark) {
        return MessageCommand.newBuilder()
                .setRpcType(RESPONSE)
                .setCode(code)
                .setOpaque(opaque)
                .setRemark(remark)
                .setVersion(getVersion())
                .build();
    }

    public static MessageCommand createResponseCommandFail(int opaque) {
        return createResponseCommand(RemotingSysResponseCode.SYSTEM_ERROR,
                opaque,
                "not set any response code");
    }

    public static MessageCommand createResponseCommandSuccess(int opaque) {
        return MessageCommand.newBuilder()
                .setCode(ResponseCode.SUCCESS)
                .setOpaque(opaque)
                .setRpcType(RESPONSE)
                .setVersion(getVersion())
                .build();
    }

    public static MessageCommand.Builder createResponseBuilder(int opaque) {
        return MessageCommand.newBuilder()
                .setCode(ResponseCode.SUCCESS)
                .setOpaque(opaque)
                .setRpcType(RESPONSE)
                .setVersion(getVersion());
    }

    public static MessageCommand.Builder createResponseBuilder(int opaque, byte[] body) {
        MessageCommand.Builder builder = MessageCommand.newBuilder()
                .setCode(ResponseCode.SUCCESS)
                .setOpaque(opaque)
                .setRpcType(RESPONSE)
                .setVersion(getVersion());

        if (ArrayUtils.isNotEmpty(body)) {
            builder.setBody(ByteString.copyFrom(body));
        }

        return builder;
    }

    public static MessageCommand createResponseCommand(byte[] body, int opaque) {
        return createResponseBuilder(opaque, body).build();
    }

    public static MessageCommand createRequestCommand(int code) {
        return createRequestCommand(code, "");
    }

    public static MessageCommand createRequestCommand(int code, String remark) {
        return MessageCommand.newBuilder()
                .setRpcType(REQUEST)
                .setCode(code)
                .setVersion(getVersion())
                .setRemark(remark)
                .setOpaque(RequestId.getAndIncrement())
                .build();
    }

    public static MessageCommand.Builder createRequestBuiler(int code) {
        return MessageCommand.newBuilder()
                .setRpcType(REQUEST)
                .setCode(code)
                .setVersion(getVersion())
                .setOpaque(RequestId.getAndIncrement());
    }

    public static MessageCommand.Builder createRequestBuiler(int code, byte[] body) {
        MessageCommand.Builder builder = MessageCommand.newBuilder()
                .setRpcType(REQUEST)
                .setCode(code)
                .setVersion(getVersion())
                .setOpaque(RequestId.getAndIncrement());

        if (ArrayUtils.isNotEmpty(body)) {
            builder.setBody(ByteString.copyFrom(body));
        }

        return builder;
    }

    public static boolean isOneWayRpc(MessageCommand cmd) {
        return cmd.hasRpcType() && cmd.getRpcType().equals(Command.RpcType.ONE_WAY);
    }

    public static int getVersion() {
        if (ConfigVersion >= 0) {
            return ConfigVersion;
        } else {
            String v = System.getProperty(RemotingVersionKey);
            if (v != null) {
                int value = Integer.parseInt(v);
                ConfigVersion = value;

                return ConfigVersion;
            }
        }

        return 0;
    }
}
