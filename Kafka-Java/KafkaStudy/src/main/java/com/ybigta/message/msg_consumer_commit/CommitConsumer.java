package com.ybigta.message.msg_consumer_commit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class CommitConsumer {

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("session.timeout.ms", "10000");
        configs.put("group.id", "JavaCommitConsumer");
        configs.put("enable.auto.commit", "false");
        configs.put("auto.offset.reset", "earliest");
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);
        consumer.subscribe(Arrays.asList("topic-180824"));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.topic());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println("");
                }

                try {
                    consumer.commitSync();
                }catch (Exception e){
                    System.out.println("Commit Fail");
                }
            }

        }catch (Exception e){
            System.out.println("CommitMsg Error");
            System.out.println(e.getMessage());
            System.out.println("");

        }finally {
            System.out.println("CommitMsg Finish");
            consumer.close();
        }
    }
}