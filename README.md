# kafka-hadoop-consumer
Another kafka-hadoop-consumer, but this one uses Zookeeper!

Originally forked from: https://github.com/miniway/kafka-hadoop-consumer

##  Changes


- ```package_deb.sh``` uses Maven and FPM to build a .deb package.
This requires that you have Kafka installed via the WMF-Analytics
Kraken apt repository: http://analytics1001.wikimedia.org:81/apt/

```bash
# cat /etc/apt/sources.list.d/kraken.list 
deb http://analytics1001.wikimedia.org:81/apt binary/
deb-src http://analytics1001.wikimedia.org:81/apt source/
```

- ```pom.xml``` has been modified to use CDH4 and updated Zookeeper dependencies.

- Added wrapper shell script ```kafka-hadoop-consumer```

## To build:
```bash
./package_deb.sh
```

## Usage
```bash
$ java -cp target/hadoop_consumer-1.0-SNAPSHOT.jar:`hadoop classpath` kafka.consumer.HadoopConsumer -z <zookeeper> -t <topic> target_hdfs_path
```

OR:
```bash
kafka-hadoop-consumer -t <topic> -o <hadoop_output_dir> -g <kafka_consumer_group> [-z <zookeeper_connection>] [-l <limit>]

Options:
  -t <topic>                Kafka topic from which to consume.
  -o <hadoop_output_dir>    HDFS directory in which to store consumed messages.
  -g <kafka_consumer_group> ID of consumer group in ZooKeeper under which to save consuemd offset.
  -z <zookeeper_connection> ZooKeeper connection string.  Comma separated list of hosts:ports.  Default: localhost:2181
  -l <limit>                Max number of messages to consume.  Default: -1 (the topic will be consumed from the current offset to the end)
```