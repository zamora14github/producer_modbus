package org.plc4x.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class Producer {

    private KafkaProducer<String, String> producer;

    public KafkaProducer<String, String> getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer<String, String> producer) {
        this.producer = producer;
    }

    public Producer(){
        producer = createProducer();
    }

    public KafkaProducer<String, String> createProducer(){
        String bootstrapServers = "10.0.0.3:9092,10.0.0.7:9093,10.0.0.8:9094";

        // create Producer properties
        Properties properties = new Properties();
            properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create Safe Producer
            properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
            properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
            properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
            properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        // High Throughput Producer (at the expense of a bit of latency and CPU usage)
            properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy"); // snappy compression type
            properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "10"); // ms waiting before sending a batch out
            properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create and return the producer (serialized to send JSON)
             //return new KafkaProducer<>(properties);
            return new KafkaProducer<>(properties,new StringSerializer(),new KafkaJsonSerializer());
        }

}
