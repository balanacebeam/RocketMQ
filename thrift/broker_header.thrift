namespace java com.alibaba.rocketmq.common.protocol.thrift

struct CheckTransactionStateRequestHeader {
    1: required i64 tranStateTableOffset,
    2: required i64 commitLogOffset;
    3: optional string msgId,
    4: optional string transactionId
}

struct CloneGroupOffsetRequestHeader {
    1: required string srcGroup,
    2: required string destGroup,
    3: optional string topic,
    4: optional bool offline
}

struct ConsumeMessageDirectlyResultRequestHeader {
    1: required string consumerGroup,
    2: optional string clientId,
    3: optional string msgId,
    4: optional string brokerName
}

struct ConsumerSendMsgBackRequestHeader {
    1: required i64 offset,
    2: required string group,
    3: required i32 delayLevel,
    4: optional bool unitMode = false,
    5: optional string originMsgId,
    6: optional string originTopic
}

enum TopicFilterType {
    SINGLE_TAG = 1,
    MULTI_TAG = 2
}
struct CreateTopicRequestHeader {
    1: required string topic,
    2: required string defaultTopic,
    3: required i32 readQueueNums,
    4: required i32 writeQueueNums,
    5: required i32 perm,
    6: required TopicFilterType topicFilterType,
    7: required bool order = false,
    8: optional i32 topicSysFlag
}

struct DeleteSubscriptionGroupRequestHeader {
    1: required string groupName
}

struct DeleteTopicRequestHeader {
    1: required string topic
}

enum TransactionType {
    NONE = 0,
    PREPARED = 4,
    COMMIT = 8,
    ROLLBACK = 12
}
struct EndTransactionRequestHeader {
    1: required string producerGroup,
    2: required i64 tranStateTableOffset,
    3: required i64 commitLogOffset,
    4: required TransactionType commitOrRollback,
    5: optional bool fromTransactionCheck = false,
    6: required string msgId,
    7: optional string transactionId
}

struct GetBrokerConfigResponseHeader {
    1: required string version
}

struct GetConsumerConnectionListRequestHeader {
    1: required string consumerGroup
}

struct GetConsumerListByGroupRequestHeader {
    1: required string consumerGroup
}

struct GetConsumerRunningInfoRequestHeader {
    1: required string consumerGroup,
    2: required string clientId,
    3: optional bool jstackEnable
}

struct GetConsumerStatusRequestHeader {
    1: required string topic,
    2: required string group,
    3: optional string clientAddr
}

struct GetConsumeStatsRequestHeader {
    1: required string consumerGroup,
    2: optional string topic
}

struct GetEarliestMsgStoreTimeRequestHeader {
    1: required string topic,
    2: required i32 queueId
}

struct GetEarliestMsgStoreTimeResponseHeader {
    1: required i64 timestamp
}

struct GetMaxOffsetRequestHeader {
    1: required string topic,
    2: required i32 queueId
}

struct GetMaxOffsetResponseHeader {
    1: required i64 offset
}

struct GetMinOffsetRequestHeader {
    1: required string topic,
    2: required i32 queueId
}

struct GetMinOffsetResponseHeader {
    1: required i64 offset
}

struct GetProducerConnectionListRequestHeader {
    1: required string producerGroup
}

struct GetTopicsByClusterRequestHeader {
    1: required string cluster
}

struct GetTopicStatsInfoRequestHeader {
    1: required string topic
}

struct NotifyConsumerIdsChangedRequestHeader {
    1: required string consumerGroup
}

struct PullMessageRequestHeader {
    1: required string consumerGroup,
    2: required string topic,
    3: required i32 queueId,
    4: required i64 queueOffset,
    5: required i32 maxMsgNums,
    6: required i32 sysFlag,
    7: required i64 commitOffset,
    8: required i64 suspendTimeoutMillis,
    9: required i64 subVersion,
    10: optional string subscription
}

struct PullMessageResponseHeader {
    1: required i64 suggestWhichBrokerId,
    2: required i64 nextBeginOffset,
    3: required i64 minOffset,
    4: required i64 maxOffset
}

struct QueryConsumerOffsetRequestHeader {
    1: required string consumerGroup,
    2: required string topic,
    3: required i32 queueId
}

struct QueryConsumerOffsetResponseHeader {
    1: required i64 offset
}

struct QueryConsumeTimeSpanRequestHeader {
    1: required string topic,
    2: required string group
}

struct QueryCorrectionOffsetHeader {
    1: required string compareGroup,
    2: required string topic,
    3: optional string filterGroups
}

struct QueryMessageRequestHeader {
    1: required string topic,
    2: required string key,
    3: required i32 maxNum,
    4: required i64 beginTimestamp,
    5: required i64 endTimestamp
}

struct QueryMessageResponseHeader {
    1: required i64 indexLastUpdateTimestamp,
    2: required i64 indexLastUpdatePhyOffset
}

struct QueryTopicConsumeByWhoRequestHeader {
    1: required string topic
}

struct ResetOffsetRequestHeader {
    1: required string topic,
    2: required string group,
    3: required i64 timestamp,
    4: required bool isForce
}

struct SearchOffsetRequestHeader {
    1: required string topic,
    2: required i32 queueId,
    3: required i64 timestamp
}

struct SearchOffsetResponseHeader {
    1: required i64 offset
}

struct SendMessageRequestHeader {
    1: required string producerGroup,
    2: required string topic,
    3: required string defaultTopic,
    4: required i32 defaultTopicQueueNums,
    5: required i32 queueId,
    6: required i32 sysFlag,
    7: required i64 bornTimestamp,
    8: required i32 flag,
    9: optional string properties,
    10: optional i32 reConsumeTimes,
    11: optional bool unitMode = false
}

struct SendMessageResponseHeader {
    1: required string msgId,
    2: required i32 queueId,
    3: required i64 queueOffset,
    4: optional string transactionId
}

struct UnregisterClientRequestHeader {
    1: required string clientId,
    2: optional string producerGroup,
    3: optional string consumerGroup
}

struct UpdateConsumerOffsetRequestHeader {
    1: required string consumerGroup,
    2: required string topic,
    3: required i32 queueId,
    4: required i64 commitOffset
}

struct ViewBrokerStatsDataRequestHeader {
    1: required string statsName,
    2: required string statsKey
}

struct ViewMessageRequestHeader {
    1: required i64 offset
}