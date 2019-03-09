package com.yq.processor;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

/**
 * Simple to Introduction
 * className: ProcessMain
 *
 * @author EricYang
 * @version 2019/3/7 13:47
 */
public class ProcessMain {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", "src-topic")
                .addProcessor("PROCESS1", MyProcessor::new, "SOURCE")
                .addStateStore(Stores.create("Counts").withStringKeys().withStringValues().inMemory().build(), "PROCESS1")
                .addSink("SINK", "sink-topic", "PROCESS1");



        StateStoreSupplier countStore = Stores.create("Counts")
                .withKeys(Serdes.String())
                .withValues(Serdes.Long())
                .persistent()
                .build();
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-temperature");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "10.76.3.70:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


    }

}
