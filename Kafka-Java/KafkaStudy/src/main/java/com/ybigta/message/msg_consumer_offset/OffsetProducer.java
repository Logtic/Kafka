package com.ybigta.message.msg_consumer_offset;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class OffsetProducer {
    public static void main(String[] args) throws IOException {

        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        for (int i = 0; i < 100; i++) {
            String v = "Java OffsetProducer msg-"+i;
            producer.send(new ProducerRecord<String, String>("topic-180824", v));
            System.out.println("OffsetProducer Send Msg");
        }

        producer.flush();
        producer.close();
    }
}
