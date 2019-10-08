# oss-kafka-cassandra

This application is an example of how to create a Kafka producer and a consumer where the consumer is able to both consume messages and in turn write the consumed records to a data source.  For this example the data source is Cassandra, but the code can be modified to write data to any number of data sources such as MySQL or Postgres.   

## Prerequisites:

In order to run this example it is necessary to have a Kafka single or multi node cluster as well as Cassandra single node or multi node cluster that is SSL enabled.  The source code uses a .pem file to access a Cassandra cluster using SSL.  It will be necessary to recompile the code adding your specific environment values.  For example, the CassandraConnector class has a connect() method where you pass in values specific to your environment.  

### Compile and Package

```
mvn compile package
```
### Step 1 - produce messages

```
java -jar oss-kafka-cassandra-1.0.jar producer videos 127.0.0.1:9092 VideoProducer a1
```
### Step 2 - view / check messages

```
kafka-console-consumer.sh --bootstrap-server 127.0.0.1:9092 --topic videos --from-beginning
```
### Step 3 - consume and write messages

```
java -jar oss-kafka-cassandra-1.0.jar consumer videos 127.0.0.1:9092 VideoConsumerAndSenderWithThread a2
```
### Additional

To remove the topic and repeat the above steps use the following command. 

```
kafka-topics.sh --zookeeper 127.0.0.1:2181 --topic videos --delete
```



