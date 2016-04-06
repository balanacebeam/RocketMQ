namespace java com.alibaba.rocketmq.common.protocol.thrift

struct RegisterFilterServerRequestHeader {
    1: required string filterServerAddr
}

struct RegisterFilterServerResponseHeader {
    1: required string brokerName,
    2: required i64 brokerId
}

struct RegisterMessageFilterClassRequestHeader {
    1: required string consumerGroup,
    2: required string topic,
    3: required string className,
    4: required i32 classCRC
}