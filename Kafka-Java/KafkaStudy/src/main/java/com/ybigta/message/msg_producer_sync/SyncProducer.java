package com.ybigta.message.msg_producer_sync;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

public class SyncProducer {
    public static void main(String[] args) throws IOException {

        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        try{
            String v = "Java SyncProducer SyncMsg-"+1;
            RecordMetadata metadata = producer.send(new ProducerRecord<String, String>("topic-180824", v)).get();
            System.out.println("SyncMsgProducer Send Msg");
            System.out.println(metadata.partition());
            System.out.println(metadata.offset());
            System.out.println("");

        }catch (Exception e){
            System.out.println("SyncMsg Error");
            System.out.println(e.getMessage());
            System.out.println("");

        }finally {
            System.out.println("SyncMsg Finish");
            producer.close();
        }
    }
}
