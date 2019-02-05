1. Start ZooKeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

2. Start Kafka
bin/kafka-server-start.sh config/server.properties

3. Create input topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic customer-transactions

4. Create output topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic transactions-aggregate --config cleanup.policy=compact

5. Start a Kafka consumer to monitor input topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic customer-transactions \
        --from-beginning \
        --formatter kafka.tools.DefaultMessageFormatter \
        --property print.key=true \
        --property print.value=true \
        --property key.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer \
        --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer

6. Start a Kafka consumer to monitor output topic
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic transactions-aggregate \
    --from-beginning \
    --formatter kafka.tools.DefaultMessageFormatter \
    --property print.key=true \
    --property print.value=true \
    --property key.deserializer=org.apache.kafka.common.serialization.IntegerDeserializer \
    --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer