package com.ybigta.message.msg_producer_async;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.IOException;
import java.util.Properties;

public class AsyncProducer {
    public static void main(String[] args) throws IOException {

        Properties configs = new Properties();
        configs.put("bootstrap.servers", "13.209.15.2:9092");
        configs.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        configs.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(configs);

        try{
            String v = "Java Producer AsyncMsg-"+1;
            producer.send(new ProducerRecord<String, String>("topic-180824", v), new AsyncCallback());
            System.out.println("AsyncMsgProducer Send Msg");

        }catch (Exception e){
            System.out.println("AsyncMsg Error");
            System.out.println(e.getMessage());
            System.out.println("");

        }finally {
            System.out.println("AsyncMsg Finish");
            producer.close();
        }
    }
}

class AsyncCallback implements org.apache.kafka.clients.producer.Callback{
    @Override
    public void onCompletion(RecordMetadata metadata, Exception e) {
        if (metadata != null){
            System.out.println(metadata.partition());
            System.out.println(metadata.offset());
            System.out.println(" ");
        }else {
            System.out.println(e.getMessage());
        }
    }
}
