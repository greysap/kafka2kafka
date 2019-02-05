package house.greysap;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Kafka Producer that sends records with customer transactions.
 */
public class TransactionsProducer {
    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "3");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "1");
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        Producer<Integer, String> producer = new KafkaProducer<>(properties);

        while (true) {
            try {
                producer.send(newRandomTransaction(123, 100.0));
                Thread.sleep(1000);
                producer.send(newRandomTransaction(321, -10.0));
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                break;
            }
        }
        producer.close();
    }

    private static ProducerRecord<Integer, String> newRandomTransaction(int customerId, double amount) {
        long transactionId = ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
        long transactionTime = Instant.now().toEpochMilli();

        ObjectNode transaction = JsonNodeFactory.instance.objectNode();
        transaction.put("customerId", customerId);
        transaction.put("amount", amount);
        transaction.put("timestamp", transactionTime);
        transaction.put("transactionId", transactionId);

        return new ProducerRecord<>("customer-transactions", customerId, transaction.toString());
    }
}