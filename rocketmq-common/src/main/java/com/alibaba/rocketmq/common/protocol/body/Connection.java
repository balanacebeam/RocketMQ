package com.alibaba.rocketmq.common.protocol.body;


/**
 * TODO
 *
 * @author shijia.wxr<vintage.wang@gmail.com>
 * @since 13-8-5
 */
public class Connection {
    private String clientId;
    private String clientAddr;
    private int version;


    public String getClientId() {
        return clientId;
    }


    public void setClientId(String clientId) {
        this.clientId = clientId;
    }


    public String getClientAddr() {
        return clientAddr;
    }


    public void setClientAddr(String clientAddr) {
        this.clientAddr = clientAddr;
    }


    public int getVersion() {
        return version;
    }


    public void setVersion(int version) {
        this.version = version;
    }
}
