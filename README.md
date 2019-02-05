# kafka2kafka (work in progress)
## Simple example of using Kafka Streams for imaginary case of aggregating customers bank transactions (similar to microbatch2cassandra)

### Case description
Our customer can make debit and credit transactions. Transactions arrive as JSON-messages to Kafka.
Our task is to aggregate all of them within configurable time windows and save aggregates to a Kafka topic.
"To aggregate" means to group transactions by customer and sum up.

### Few complications:
1. Transactions may be incorrect (malformed, zero amount).
2. Transaction may be duplicated in one hour. And we mustn't sum up one transaction twice.
3. Transactions may be late for few hours, but no more than 24 hours.

### Solution:
1. Use Kafka Streams!
