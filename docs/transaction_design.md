## 事务日志表设计
> http
It offers a variety of features as follows:
gogo

| 属性名          | 类型           | 长度      | 备注  |
| :--------------: |:--------------:| :-----:| :-----|
| broker_name    | varchar        | 30   |配置文件里的brokerName，一定要配置，而且不要改，可以按照该字段进行分库|
| offset         | bigint         |      |broker内commitLog的offset，可以使用该字段进行分表，也可以按照时间进行分表|
| producer_group | varchar        |  50  |生产者配置的group，不要轻易修改，如果更改也要进行版本兼容，要不然事务回查会有问题|
| gmt_create     | datetime       |      |日志创建时间|

1. prepare事务日志插入
2. rollback事务日志删除
3. commit事务日志删除
4. check事务回查
	* 遍历当前broker的XX秒之内的prepare事务日志，并通过offset拿到该消息的详情，通过producer_group拿到机器ip进行回查
	* 每个broker单线程定时程序回查prepare事务日志
5. 事务日志redo
	* 由于requestDispatch中的消息消费顺序跟commitLog的顺序是一样的，所以在每次事务日志成功写入db之后更新当前事务日志的checkpoint（此处可以进行批量处理之后再记录checkpoint）
	* 事务日志回滚的时候，拿到所有checkpoint的最小值进行recovery，重新走dispatch流程
