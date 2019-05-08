package com.yq;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 * cfg
   {"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2}

 {"deviceId":"001", "chainId":"c1", "nodeId":"n1", "cfg":{"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2},"data":{"T1031":35, "T1032":55}, "ts":234843}
 * @author EricYang
 * @version 2019/4/28 19:16
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class KafkaAccProdMain {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";
    private static final String FLINK_BROKERS = "127.0.0.1";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(FLINK_BROKERS, 8081);

        // 属性参数 - 实际投产可以在命令行传入
        Map<String, String> map = new HashMap<>();
        map.put("bootstrap.servers", KAFKA_BROKERS);
        map.put("group.id", "flink-kafka-connector");
        final ParameterTool parameterTool = ParameterTool.fromMap(map);
        env.getConfig().setGlobalJobParameters(parameterTool);


        env.execute("Kafka remote submit jar");
    }
}