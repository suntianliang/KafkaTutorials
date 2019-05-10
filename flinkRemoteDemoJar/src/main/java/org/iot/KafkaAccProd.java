package org.iot;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 * cfg
   {"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2}

 {"deviceId":"001", "chainId":"c1", "nodeId":"n1", "cfg":{"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2},"data":{"T1031":35, "T1032":55}, "ts":234843}
  运行参数    --bootstrap.servers 127.0.0.1:9092 --limitEnabled 2 --timeLimit 2 --group.id grp01 --nodeId a1b2c3
 * @author EricYang
 * @version 2019/4/28 19:16
 */

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Map;
import java.util.Properties;

@Slf4j
public class KafkaAccProd {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);
        ExecutionConfig.GlobalJobParameters parameters = env.getConfig().getGlobalJobParameters();
        Map<String, String> map = parameters.toMap();

        String sourceTopic = map.get("sourceTopic");
        String sinkTopic =  map.get("sinkTopic");
        log.info("bootstrap.servers={}, sourceTopic={}, sinkTopic={}", map.get("bootstrap.servers"), sourceTopic, sinkTopic);
        String nodeId = map.get("nodeId");
        log.info("group.id={}, LimitEnabled={}, nodeId={}", map.get("group.id"), map.get("limitEnabled"), nodeId);


        Properties properties = parameterTool.getProperties();
        properties.putAll(parameterTool.getProperties());
        properties.put("bootstrap.servers", map.get("bootstrap.servers"));
        properties.put("group.id", map.get("group.id"));

        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                sourceTopic,
                new SimpleStringSchema(),
                properties);

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);


        int limitType = Integer.valueOf(map.get("limitEnabled"));
        if (limitType == 2) {
            long windowSize = Long.valueOf(map.get("timeLimit"));
            DataStream<String> windowCounts = input.map(new MapFunction<String, String>() {
                private static final long serialVersionUID = -6867736771747690202L;
                @Override
                public String map(String value) throws Exception {
                    log.info("map_msg={}", value);
                    return value;
                }
            })
                    .timeWindowAll(Time.minutes(windowSize))
                    .fold(new String("{}"), new MyFoldFunction());

            // 创建生产者
            FlinkKafkaProducer myProducer = new FlinkKafkaProducer<String>(
                    sinkTopic,
                    new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                    properties,
                    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
            myProducer.setWriteTimestampToKafka(true);
            windowCounts.addSink(myProducer);
        }
        else if (limitType == 3) {
            long countSize = Long.valueOf(map.get("countLimit"));

            DataStream<String> windowCounts = input.map(new MapFunction<String, String>() {
                private static final long serialVersionUID = -6867736771747690202L;
                @Override
                public String map(String value) throws Exception {
                    log.info("map_msg={}", value);
                    return value;
                }
            })
                    .countWindowAll(countSize)
                    .fold(new String("{}"), new MyFoldFunction());

            // 创建生产者
            FlinkKafkaProducer myProducer = new FlinkKafkaProducer<String>(
                    sinkTopic,
                    new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                    properties,
                    FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
            myProducer.setWriteTimestampToKafka(true);
            windowCounts.addSink(myProducer);
        }
        else {
            log.error("invalid limitType={} for it is not 2 or 3", limitType);
            return;
        }

        // 执行job
        env.execute("remoteJar_" + nodeId);
    }
}