flume-kudu-sink
=====================

<p> flume关于kudu的sink组件</p>  

#### 更新点  
* 衔接json字符串转换source
* 根据传输的Json字段创建表

#### 具体用法  
* 项目根目录下执行mvn clean package
* 将tar/flume.kudu.sink-1.0-SNAPSHOT.jar文件拷贝到flume-ng根目录下的lib文件夹中
* 启动Flume:nohup ./bin/flume-ng agent --conf ./conf --conf-file log_t.conf --name agent -Dflume.root.logger=INFO,console >  /dev/null 2>&1 &
###### 注意  
-Dflume.root.logger传递的是log4j.properties文件中的参数，例如如果想每个实例一个文件，那么传递-Dflume.log.file=haha.log

#### 用法示例  
```
agent.channels = fileChannel
agent.sources = sqlSource
agent.sinks = kuduSink

agent.sources.sqlSource.type = org.keedio.flume.source.SQLSource

agent.sources.sqlSource.hibernate.connection.url = jdbc:mysql://127.0.0.1:3306/test?autoReconnect=true

agent.sources.sqlSource.hibernate.connection.user = test 
agent.sources.sqlSource.hibernate.connection.password = test
agent.sources.sqlSource.hibernate.connection.autocommit = true
agent.sources.sqlSource.hibernate.dialect = org.hibernate.dialect.MySQL5Dialect
agent.sources.sqlSource.hibernate.connection.driver_class = com.mysql.jdbc.Driver
agent.sources.sqlSource.hibernate.temp.use_jdbc_metadata_defaults=false

agent.sources.sqlSource.table = items
# Columns to import to kafka (default * import entire row)
agent.sources.sqlSource.columns.to.select = *
# Query delay, each configured milisecond the query will be sent
agent.sources.sqlSource.run.query.delay= 300000
# Status file is used to save last readed row
agent.sources.sqlSource.status.file.path = /data/flume_status
agent.sources.sqlSource.status.file.name = items

# Custom query
agent.sources.sqlSource.start.from = 0 
agent.sources.sqlSource.source.transfer.method = bulk

agent.sources.sqlSource.batch.size = 3000
agent.sources.sqlSource.max.rows = 10000

agent.sources.sqlSource.hibernate.connection.provider_class = org.hibernate.connection.C3P0ConnectionProvider
agent.sources.sqlSource.hibernate.c3p0.min_size=1
agent.sources.sqlSource.hibernate.c3p0.max_size=3

# The channel can be defined as follows.
agent.channels.fileChannel.type = file
agent.channels.fileChannel.checkpointDir = /data/flume_checkpont/items
agent.channels.fileChannel.dataDirs = /data/flume_datadirs/items
agent.channels.fileChannel.capacity = 200000000
agent.channels.fileChannel.keep-alive = 180
agent.channels.fileChannel.write-timeout = 180
agent.channels.fileChannel.checkpoint-timeout = 300
agent.channels.fileChannel.transactionCapacity = 1000000

agent.sources.sqlSource.channels = fileChannel
agent.sinks.kuduSink.channel = fileChannel

######配置kudu sink ##############################
agent.sinks.kuduSink.type = com.flume.sink.kudu.KuduSink
agent.sinks.kuduSink.masterAddresses = 127.0.0.1:7051,127.0.0.2:7051,127.0.0.3:7051
agent.sinks.kuduSink.customKey = itemid
agent.sinks.kuduSink.tableName = items
agent.sinks.kuduSink.batchSize = 100
agent.sinks.kuduSink.namespace = test
agent.sinks.kuduSink.producer.operation = upsert
agent.sinks.kuduSink.producer = com.flume.sink.kudu.JsonKuduOperationProducer

```
Configuration of KuDu Sink:  
---------------------------
| Property Name | Default | Description |
| ----------------------- | :-----: | :---------- |
| masterAddresses | - | kudu master地址 |
| type | - | sink类型 |
| customKey | - | 指定的表的主键 |
| tableName | - | kudu表名 |
| batchSize | 100 | sink每批次处理数 |
| namespace | - | kudu表前缀 |
| producer.operation | - | kudu表插入操作类型 |
| producer | com.flume.sink.kudu.JsonKuduOperationProducer | kudu数据操作具体执行类 |
