# kafka-read-all

Sometimes it can be useful to read all the messages inside a Kafka topic. In this repo we provide an example of producer and consumer. The messages are produced in batches. The consumer calculates lag and gets as many messages as the lag value.