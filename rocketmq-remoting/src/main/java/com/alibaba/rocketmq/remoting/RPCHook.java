package com.alibaba.rocketmq.remoting;

import com.alibaba.rocketmq.common.protocol.protobuf.Command.MessageCommand;

public interface RPCHook {
    public void doBeforeRequest(final String remoteAddr, final MessageCommand request);


    public void doAfterResponse(final String remoteAddr, final MessageCommand request,
                                final MessageCommand response);
}
