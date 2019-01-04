package com.ybigta.streaming.wordcount;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class WordCountProcessor implements Processor<String, String> {
    KeyValueStore<String, Long> store;
    ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        store = (KeyValueStore<String, Long>) context.getStateStore("Counts");
    }

    @Override
    public void process(String key, String value) {
        String[] words = value.toLowerCase(Locale.getDefault()).split("\\W+");

        for (String word : words) {
            Long oldValue = store.get(word);

            if (oldValue == null) {
                store.put(word, 1L);
                context.forward(word, "1");
            } else {
                store.put(word, oldValue + 1);
                context.forward(word, String.valueOf(oldValue));
            }
        }
        context.commit();
    }

    @Override
    public void punctuate(long l) {
        KeyValueIterator<String, Long> iter = store.all();

        while (iter.hasNext()){
            KeyValue<String, Long> kv = iter.next();
            System.out.println("KEY: "  + kv.key + " VALUE: " + kv.value);
        }
    }

    @Override
    public void close() {
    }

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "13.209.15.2:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology builder = new Topology();



        builder.addSource("Source", "streams-wordcount-processor-input")
                .addProcessor("Process", WordCountProcessor::new, "Source")
                .addStateStore(Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Counts"),
                        Serdes.String(),
                        Serdes.Long()),
                        "Process")
                .addSink("Sink", "streams-wordcount-processor-output", "Process");

        KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook"){
            @Override
            public void run() {
                streams.close();
                System.out.println("Topology started");
                latch.countDown();
                System.out.println("Topology stopped");
            }
        });

        try{
            streams.start();
            latch.await();
        }catch (Throwable e){
            System.exit(1);
        }
        System.exit(0);
    }
}