package com.yq.customized;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Simple to Introduction
 * className: StatisticsSerde
 *
 * @author EricYang
 * @version 2019/3/9 12:00
 */
public class StatisticsSerde implements Serde<Statistics> {
    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer serializer() {
        return new StatisticsSerializer();
    }

    @Override
    public Deserializer deserializer() {
        return new StatisticsDeserializer();
    }
}
