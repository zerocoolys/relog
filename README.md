# relog-storm

### 采用kafka分布式消息进行流量统计处理

引入storm，讲relog-consumer的处理逻辑依托给storm.

目前的想法是：

1. storm-kafka KafkaSpout 读入数据
2. Bolt: CommonsBolt 常规流量计算
         GaBolt     同类群组流量计算
         ExitRateBolt   退出率流量计算
         ElastisearchBolt   Es数据写入
         MongodbBolt    Mongodb数据写入
         
         
3. 设计思路（很简单）


    KafkaSpout -> CommonsBolt -> ElasticsearchBolt     
              |              |-> MongodbBolt (可选)                             
              | -> GaBolt      -> MongodbBolt 
              | -> ExitRateBolt -> MongodbBolt 
   
