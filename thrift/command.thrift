namespace java com.alibaba.rocketmq.common.protocol.thrift

// broker_header
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

// namesrv_header
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

// filtersrv_header
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

// common
enum RpcType {
    ONE_WAY = 1,
    TWO_WAY = 2
}

union MessageHeader {
    1: CheckTransactionStateRequestHeader checkTransactionStateRequestHeader,
    2: CloneGroupOffsetRequestHeader cloneGroupOffsetRequestHeader,
    3: ConsumeMessageDirectlyResultRequestHeader consumeMessageDirectlyResultRequestHeader,
    4: ConsumerSendMsgBackRequestHeader consumerSendMsgBackRequestHeader,
    5: CreateTopicRequestHeader createTopicRequestHeader,
    6: DeleteKVConfigRequestHeader deleteKVConfigRequestHeader,
    7: DeleteSubscriptionGroupRequestHeader deleteSubscriptionGroupRequestHeader,
    8: DeleteTopicInNamesrvRequestHeader deleteTopicInNamesrvRequestHeader,
    9: DeleteTopicRequestHeader deleteTopicRequestHeader,
    10: EndTransactionRequestHeader endTransactionRequestHeader,
    11: GetBrokerConfigResponseHeader getBrokerConfigResponseHeader,
    12: GetConsumerConnectionListRequestHeader getConsumerConnectionListRequestHeader,
    13: GetConsumerListByGroupRequestHeader getConsumerListByGroupRequestHeader,
    14: GetConsumerRunningInfoRequestHeader getConsumerRunningInfoRequestHeader,
    15: GetConsumerStatusRequestHeader getConsumerStatusRequestHeader,
    16: GetConsumeStatsRequestHeader getConsumeStatsRequestHeader,
    17: GetEarliestMsgStoreTimeRequestHeader getEarliestMsgStoreTimeRequestHeader,
    18: GetEarliestMsgStoreTimeResponseHeader getEarliestMsgStoreTimeResponseHeader,
    19: GetKVConfigRequestHeader getKVConfigRequestHeader,
    20: GetKVConfigResponseHeader getKVConfigResponseHeader,
    21: GetKVListByNamespaceRequestHeader getKVListByNamespaceRequestHeader,
    22: GetMaxOffsetRequestHeader getMaxOffsetRequestHeader,
    23: GetMaxOffsetResponseHeader getMaxOffsetResponseHeader,
    24: GetMinOffsetRequestHeader getMinOffsetRequestHeader,
    25: GetMinOffsetResponseHeader getMinOffsetResponseHeader,
    26: GetProducerConnectionListRequestHeader getProducerConnectionListRequestHeader,
    27: GetRouteInfoRequestHeader getRouteInfoRequestHeader,
    28: GetTopicsByClusterRequestHeader getTopicsByClusterRequestHeader,
    29: GetTopicStatsInfoRequestHeader getTopicStatsInfoRequestHeader,
    30: NotifyConsumerIdsChangedRequestHeader notifyConsumerIdsChangedRequestHeader,
    31: PullMessageRequestHeader pullMessageRequestHeader,
    32: PullMessageResponseHeader pullMessageResponseHeader,
    33: PutKVConfigRequestHeader putKVConfigRequestHeader,
    34: QueryConsumerOffsetRequestHeader queryConsumerOffsetRequestHeader,
    35: QueryConsumerOffsetResponseHeader queryConsumerOffsetResponseHeader,
    36: QueryConsumeTimeSpanRequestHeader queryConsumeTimeSpanRequestHeader,
    37: QueryCorrectionOffsetHeader queryCorrectionOffsetHeader,
    38: QueryMessageRequestHeader queryMessageRequestHeader,
    39: QueryMessageResponseHeader queryMessageResponseHeader,
    40: QueryTopicConsumeByWhoRequestHeader queryTopicConsumeByWhoRequestHeader,
    41: RegisterBrokerRequestHeader registerBrokerRequestHeader,
    42: RegisterBrokerResponseHeader registerBrokerResponseHeader,
    43: RegisterFilterServerRequestHeader registerFilterServerRequestHeader,
    44: RegisterFilterServerResponseHeader registerFilterServerResponseHeader,
    45: RegisterMessageFilterClassRequestHeader registerMessageFilterClassRequestHeader,
    46: ResetOffsetRequestHeader resetOffsetRequestHeader,
    47: SearchOffsetRequestHeader searchOffsetRequestHeader,
    48: SearchOffsetResponseHeader searchOffsetResponseHeader,
    49: SendMessageRequestHeader sendMessageRequestHeader,
    50: SendMessageResponseHeader sendMessageResponseHeader,
    51: UnRegisterBrokerRequestHeader unRegisterBrokerRequestHeader,
    52: UnregisterClientRequestHeader unregisterClientRequestHeader,
    53: UpdateConsumerOffsetRequestHeader updateConsumerOffsetRequestHeader,
    54: ViewBrokerStatsDataRequestHeader viewBrokerStatsDataRequestHeader,
    55: ViewMessageRequestHeader viewMessageRequestHeader,
    56: WipeWritePermOfBrokerRequestHeader wipeWritePermOfBrokerRequestHeader,
    57: WipeWritePermOfBrokerResponseHeader wipeWritePermOfBrokerResponseHeader,
}

struct MessageCommand {
    1: required i32 code,
    2: required i16 version,
    3: required i32 opaque,
    4: required RpcType rpcType,
    5: string remark,
    6: MessageHeader header,
    7: binary body
}