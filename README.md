# spark-streaming
## Frameworks

1) Flume as ingestion tool
2) Kafka as messaging backbone
3) Spark Streaming for processing
4) Redis as lookup service and bots registry

###Flume JSON filter

This is a flume event interceptor, that cleans up incoming json string and removes unnecessary symbols.

#### How to use
1) Build the project:

```mvn clean package```
2) Copy jar file to container to ```/opt/flume/lib``` directory:

```
docker cp ./target/flume-json-filter-1.0-SNAPSHOT.jar <container_name>:/opt/flume/lib
```

### Spark streaming kafka

BotDetectorV1 is implemented using DStreams.
BotDetectorV2 is implemented using Structured Streaming.

In general, workflow of BotDetectorV1 and BotDetectorV2 are similar:
1) take events from kafka every ```window``` period of time
2) process events, gather statistics and identify bots
3) put statistics (ip -> click, view, event rates, etc.) and bots info into redis

BotDetectorV1 puts data into 2 different redis sets:
1) ```statistic``` - contains key/values with statistic for the last ```window``` period of time
2) ```bots``` - contains all identified bots

Problem with this approach is that TTL cannot be set for individual key/value pair in set but rather for the whole set.

At the same time, BotDetectorV2 uses different approach.
It puts data into redis as individual k/v pairs with specified TTL. In order to identify statistical records and bots every key has prefix:
1) ```stat_``` for statistics
2) ```bot_```  for bots

So now end-user can filter keys by prefix and get required information.

#### Configuration file
Config file path: ```spark-streaming-kafka/src/main/resources/application.conf```

Redis and Kafka properties should not be modified unless you change kafka and redis configuration.
Rest of the properties define application logic and business rules and should be modified 
according to the requirements.


#### Configure environment

1) ```cd spark-streaming-kafka/src/main/resources```
2) ```docker-compose up -d```
3) create kafka topic for logs (run from inside of the kafka container): ```bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic bot-logs```
4) copy flume interceptor jar (see Flume ```JSON filter -> how to use -> step 2```)
5) restart flume container (so now flume can see newly add interceptor)

Check out ```docker-compose.yml``` in order to find out ports for container like Redis UI or Kafka Topics UI.

#### Generate logs

In order to generate logs, run ```gridu-bd-streaming/spark-streaming-kafka/src/main/resources/botgen.py```

Example:
1) ```cd gridu-bd-streaming/spark-streaming-kafka/src/main/resources``` 
2) ```python3 botgen.py -b 100 -u 1000 -n 100 -d 60 -f ./full-stack/logs/logs-1_minute_4.json```

Don't modify output path, as far as:
1) this path is mounted into flume docker container
2) flume is configured to take new files from that directory

Check out script for more details.