package com.yq.processor;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;

import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Simple to Introduction
 * className: MyProcessor
 *
 * @author EricYang
 * @version 2019/3/7 13:44
 */
public class MyProcessor implements Processor {
    private ProcessorContext context;
    private KeyValueStore kvStore;

    @Override
    @SuppressWarnings("unchecked")
    public void init(ProcessorContext context) {
        this.context = context;
        this.context.schedule(1000);
        this.kvStore = (KeyValueStore) context.getStateStore("Counts");
    }

    @Override
    public void process(Object key, Object value) {
        String line = (String) value;
        String[] words = line.toLowerCase().split(" ");

        for (String word : words) {
            Integer oldValue = (Integer) this.kvStore.get(word);

            if (oldValue == null) {
                this.kvStore.put(word, 1);
            } else {
                this.kvStore.put(word, oldValue + 1);
            }
        }
    }


    @Override
    public void punctuate(long timestamp) {
        KeyValueIterator iter = this.kvStore.all();

        while (iter.hasNext()) {
            KeyValue entry = (KeyValue)iter.next();
            context.forward(entry.key, entry.value.toString());
        }

        iter.close();
        context.commit();
    }

    @Override
    public void close() {
        this.kvStore.close();
    }
};
