package com.ybigta.message.msg_producer_key;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

public class KeyProducer {
    public static void main(String[] args) throws IOException {

        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);
        String oddKey = "1";
        String evenKey = "2";

        for (int i = 0; i < 5; i++) {
            if(i%2==1) {
                String v = "Java KeyProducer odd msg-" + i;
                producer.send(new ProducerRecord<String, String>("topic-180824", oddKey, v));
            }else{
                String v = "Java KeyProducer even msg-" + i;
                producer.send(new ProducerRecord<String, String>("topic-180824", evenKey, v));
            }


            System.out.println("KeyProducer Send Msg");
        }

        producer.flush();
        producer.close();
    }
}
