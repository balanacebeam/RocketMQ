namespace java com.alibaba.rocketmq.common.protocol.thrift

struct DeleteKVConfigRequestHeader {
    1: required string keyspace,
    2: required string key
}

struct DeleteTopicInNamesrvRequestHeader {
    1: required string topic
}

struct GetKVConfigRequestHeader {
    1: required string keyspace,
    2: required string key
}

struct GetKVConfigResponseHeader {
    1: required string value
}

struct GetKVListByNamespaceRequestHeader {
    1: required string keyspace
}

struct GetRouteInfoRequestHeader {
    1: required string topic
}

struct PutKVConfigRequestHeader {
    1: required string keyspace,
    2: required string key,
    3: required string value
}

struct RegisterBrokerRequestHeader {
    1: required string brokerName,
    2: required string brokerAddr,
    3: required string clusterName,
    4: required string haServerAddr,
    5: required i64 brokerId
}

struct RegisterBrokerResponseHeader {
    1: required string haServerAddr,
    2: required string masterAddr
}

struct UnRegisterBrokerRequestHeader {
    1: required string brokerName,
    2: required string brokerAddr,
    3: required string clusterName,
    4: required i64 brokerId
}

struct WipeWritePermOfBrokerRequestHeader {
    1: required string brokerName
}

struct WipeWritePermOfBrokerResponseHeader {
    1: required i32 wipeTopicCount
}