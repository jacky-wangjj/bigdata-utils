消息队列

日志收集

流处理

持久性日志

依赖zookeeper

创建topic
./bin/kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 3 --topic imooc-kafka-topic

查看topic
./bin/kafka-topics --list --zookeeper localhost:2181

生产者producer
./bin/kafka-console-producer --broker-list localhost:9092 --topic imooc-kafka-topic

消费者consumer，--from-begining 重复消费所有消息
./bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic imooc-kafka-topic --from-begining

不重复消费
./bin/kafka-console-consumer --boootstrap-server localhost:9092 --topic imooc-kafka-topic
