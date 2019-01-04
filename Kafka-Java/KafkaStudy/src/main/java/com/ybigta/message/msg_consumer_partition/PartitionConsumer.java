package com.ybigta.message.msg_consumer_partition;

import kafka.common.Topic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Properties;

public class PartitionConsumer {

    public static void main(String[] args) {
        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("session.timeout.ms", "10000");
        configs.put("group.id", "JavaPartitionConsumer");
        configs.put("enable.auto.commit", "true");
        configs.put("auto.offset.reset", "earliest");
        configs.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        configs.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(configs);

        String topic = "topic-180824";
        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);
        consumer.assign(Arrays.asList(partition0, partition1));

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(1000);

                for (ConsumerRecord<String, String> record : records) {
                    System.out.println(record.topic());
                    System.out.println(record.partition());
                    System.out.println(record.offset());
                    System.out.println(record.key());
                    System.out.println(record.value());
                    System.out.println("");
                }
            }

        }catch (Exception e){
            System.out.println("PartitionMsg Error");
            System.out.println(e.getMessage());
            System.out.println("");

        }finally {
            System.out.println("PartitionMsg Finish");
            consumer.close();
        }
    }
}