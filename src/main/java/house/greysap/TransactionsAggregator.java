package house.greysap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;

/**
 * Kafka Streams application that reads customer transactions from input Kafka topic, groups them by customer,
 * sums them up and sends these aggregates to output Kafka topic.
 */
public class TransactionsAggregator {

    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "transaction-aggregator");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);

        final Serializer<JsonNode> jsonSerializer = new JsonSerializer();
        final Deserializer<JsonNode> jsonDeserializer = new JsonDeserializer();
        final Serde<JsonNode> jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        // create the initial JSON object
        ObjectNode initialAggregate = JsonNodeFactory.instance.objectNode();
        initialAggregate.put("count", 0);
        initialAggregate.put("totalAmount", 0);
        initialAggregate.put("time", Instant.ofEpochMilli(0L).toString());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, JsonNode> customerTransactions = builder.stream("customer-transactions",
                Consumed.with(Serdes.Integer(), jsonSerde));
        KTable<Windowed<Integer>, JsonNode> aggregates = customerTransactions
//                .filter() //TODO: add some filtering
                .groupByKey(Grouped.with(Serdes.Integer(), jsonSerde))
                .windowedBy(TimeWindows.of(Duration.ofMinutes(1)).grace(Duration.ofHours(1)))
                .aggregate(
                        () -> initialAggregate,
                        (key, transaction, aggregate) -> newAggregate(transaction, aggregate),
                        Materialized.<Integer, JsonNode, WindowStore<Bytes, byte[]>>as("transactions-agg")
                                .withKeySerde(Serdes.Integer())
                                .withValueSerde(jsonSerde)
                );

        aggregates.toStream().print(Printed.<Windowed<Integer>, JsonNode>toSysOut().withLabel("Customer's transactions aggregate"));

        aggregates
                .toStream()
                .map((window, jsonNode) -> KeyValue.pair(window.key(), jsonNode))
                .to("transactions-aggregate", Produced.with(Serdes.Integer(), jsonSerde));

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private static JsonNode newAggregate(JsonNode transaction, JsonNode previousAggregate) {
        // create a new aggregate json object
        ObjectNode aggregate = JsonNodeFactory.instance.objectNode();
        aggregate.put("count", previousAggregate.get("count").asInt() + 1);
        aggregate.put("totalAmount", previousAggregate.get("totalAmount").asInt() + transaction.get("amount").asInt());
        aggregate.put("time", Instant.now().toString());
        return aggregate;
    }
}