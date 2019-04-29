package com.yq.kafka.iotagg;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 *  {"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":35, "T1032":55}, "ts":234843}
 * @author EricYang
 * @version 2019/4/28 19:16
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class KafkaAggMaxMinExample {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";
    private static final long WINDOW_SIZE = 60L;
    private static final String MAX_PREFIX = "max_";
    private static final String MIN_PREFIX = "min_";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source topic
        String sourceTopic = "agg.in.abc1";
        // Sink topic
        String sinkTopic = "agg.out.abc1";

        // 属性参数 - 实际投产可以在命令行传入
        Properties properties = parameterTool.getProperties();
        properties.putAll(parameterTool.getProperties());
        properties.put("bootstrap.servers", KAFKA_BROKERS);
        properties.put("group.id", "flink-kafka-connector");

        env.getConfig().setGlobalJobParameters(parameterTool);

        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                sourceTopic,
                new KafkaMsgSchema(),
                properties);

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);

        DataStream<String> windowCounts = input.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                log.info("map_msg={}", value);
                return value;
            }
        })
                //这里使用时间窗口，也可以选择使用countWindowAll改成计数窗口
                .timeWindowAll(Time.seconds(WINDOW_SIZE))
                .fold(new String("{}"), new FoldFunction<String, String>() {
                    @Override
                    public String fold(String agg, String value) {
                       log.info("acc={}, value1={}", agg, value);

                        String ret = null;
                        try {
                            //{"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":35, "T1032":55}, "ts":234843}
                            JSONObject newMsgJson = JSON.parseObject(value);
                            //{"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "result":{"min_T1031":35, "max_T1031":35, "max_T1032":55, "max_T1032":55}, "ts":234843}
                            JSONObject aggJson = JSON.parseObject(agg);

                            JSONObject dataJson = newMsgJson.getJSONObject("data");
                            JSONObject resultAggJson = aggJson.getJSONObject("result");
                            if (dataJson != null) {
                                Set<String> fields = dataJson.keySet();
                                for(String sensorCode : fields) {
                                    long newSensorValue = dataJson.getLongValue(sensorCode);
                                    if (resultAggJson != null) {
                                        long currentMaxValue = resultAggJson.getLongValue(MAX_PREFIX + sensorCode);
                                        long currentMinValue = resultAggJson.getLongValue(MIN_PREFIX + sensorCode);
                                        if (newSensorValue > currentMaxValue) {
                                            resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                        } else if (newSensorValue < currentMinValue) {
                                            resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                        }
                                    }
                                    else {
                                        resultAggJson = new JSONObject();
                                        //如果初始化没有值就给result附上值
                                        resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                        resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);

                                        aggJson.put("chainid", newMsgJson.getString("chainid"));
                                        aggJson.put("deviceid", newMsgJson.getString("deviceid"));
                                    }

                                }
                                log.info("acc={}, value1={}, aggJson={}" , agg, value, aggJson);
                            }


                            aggJson.put("newts", System.currentTimeMillis());
                            aggJson.put("result", resultAggJson);
                            ret = aggJson.toString();
                        }
                        catch (Exception ex) {
                            log.info("toJson Exception.", ex);
                        }

                        log.info("acc={}, value1={}, ret={}" , agg, value, ret);
                        return ret;
                    }
                });

        // 创建生产者
        FlinkKafkaProducer myProducer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);
        myProducer.setWriteTimestampToKafka(true);
        windowCounts.addSink(myProducer);

        // emit result
        if (parameterTool.has("output")) {
            windowCounts.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            windowCounts.print();
        }

        // 执行job
        env.execute("Kafka Example");
    }
}