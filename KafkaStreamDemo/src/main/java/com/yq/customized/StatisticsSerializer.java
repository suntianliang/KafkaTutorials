package com.yq.customized;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * Simple to Introduction
 * className: StatisticsSerializer
 *
 * @author EricYang
 * @version 2019/3/9 11:45
 */
@Slf4j
public class StatisticsSerializer implements Serializer<Statistics> {
    private static final ObjectMapper jsonMapper = new ObjectMapper();

    @Override
    public void configure(Map map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, Statistics obj) {
        try {
            return jsonMapper.writeValueAsBytes(obj);
        }
        catch (Exception ex){
            log.error("jsonSerialize exception.", ex);
            return null;
        }
    }

    @Override
    public void close() {

    }
}
