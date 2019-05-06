package com.yq.kafka.iotagg;

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
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

@Slf4j
public class KafkaAggProdExample {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";
    private static final long WINDOW_SIZE = 120L;
    private static final String MAX_PREFIX = "MAX_";
    private static final String MIN_PREFIX = "MIN_";
    private static final String AVG_PREFIX = "AVG_";
    private static final String SUM_PREFIX = "SUM_";
    private static final String COUNT = "COUNT";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source topic
        String sourceTopic = "agg.in";
        // Sink topic
        String sinkTopic = "agg.out";

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
                            //{"deviceid":"xxx", "chainId":"yyyyy", "ruleId":"a1",
                            // "cfg":{"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2},
                            // "data":{"T1031":35, "T1032":55}, "ts":234843}
                            JSONObject newMsgJson = JSON.parseObject(value);
                            //{"deviceid":"xxx", "chainid":"yyyyy", "ruleId":"a1",
                            // "result":{"min_T1031":35, "max_T1031":35, "max_T1032":55, "max_T1032":55}, "count":15, "ts":234843}
                            JSONObject aggJson = JSON.parseObject(agg);

                            JSONObject dataJson = newMsgJson.getJSONObject("data");
                            JSONObject cfgJson = newMsgJson.getJSONObject("cfg");
                            JSONObject resultAggJson = aggJson.getJSONObject("result");
                            if (dataJson != null) {
                                Object obj = cfgJson.get("sensorCodeList");
                                JSONArray jsonArray = cfgJson.getJSONArray("sensorCodeList");
                                if (jsonArray != null) {
                                    List<String> sensorCodeList = jsonArray.toJavaList(String.class);
                                    Boolean calMAX = cfgJson.getBoolean("calMAX");
                                    Boolean calMIN = cfgJson.getBoolean("calMIN");
                                    Boolean calAVG = cfgJson.getBoolean("calAVG");
                                    Boolean calSUM = cfgJson.getBoolean("calSUM");

                                    if (resultAggJson != null) {
                                        for(String sensorCode : sensorCodeList) {
                                            long newSensorValue = dataJson.getLongValue(sensorCode);
                                            if (calMAX != null && calMAX) {
                                                long currentMaxValue = resultAggJson.getLongValue(MAX_PREFIX + sensorCode);
                                                if (newSensorValue > currentMaxValue) {
                                                    resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                                }
                                            }

                                            if (calMIN != null && calMIN) {
                                                long currentMinValue = resultAggJson.getLongValue(MIN_PREFIX + sensorCode);
                                                if (newSensorValue < currentMinValue) {
                                                    resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                                }
                                            }

                                            if (calSUM != null && calSUM) {
                                                long currentSumValue = resultAggJson.getLongValue(SUM_PREFIX + sensorCode);
                                                resultAggJson.put(SUM_PREFIX + sensorCode, currentSumValue + newSensorValue);
                                            }

                                            if (calAVG != null && calAVG) {
                                                long count = resultAggJson.getLongValue(COUNT);
                                                if (calSUM != null && calSUM) {
                                                    long currentAvgValue = resultAggJson.getLongValue(SUM_PREFIX + sensorCode);

                                                    resultAggJson.put(AVG_PREFIX + sensorCode, currentAvgValue/(count + 1));
                                                }
                                                else {
                                                    long currentAvgValue = resultAggJson.getLongValue(AVG_PREFIX + sensorCode);
                                                    long tempSum = (currentAvgValue * count) + newSensorValue;
                                                    resultAggJson.put(AVG_PREFIX + sensorCode, tempSum/(count + 1));
                                                }
                                            }
                                        }
                                    }
                                    else {
                                        resultAggJson = new JSONObject();
                                        //如果初始化没有值就给result附上值
                                        for(String sensorCode : sensorCodeList) {
                                            long newSensorValue = dataJson.getLongValue(sensorCode);
                                            if (calMAX != null && calMAX) {
                                                resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                            }

                                            if (calMIN != null && calMIN) {
                                                resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                            }

                                            if (calSUM != null && calSUM) {
                                                resultAggJson.put(SUM_PREFIX + sensorCode, newSensorValue);
                                            }

                                            if (calAVG != null && calAVG) {
                                                resultAggJson.put(AVG_PREFIX + sensorCode, newSensorValue);
                                            }
                                        }

                                        aggJson.put("chainId", newMsgJson.getString("chainId"));
                                        aggJson.put("deviceId", newMsgJson.getString("deviceId"));
                                        aggJson.put("nodeId", newMsgJson.getString("nodeId"));
                                    }

                                    long count = resultAggJson.getLongValue(COUNT);
                                    resultAggJson.put(COUNT, count + 1);
                                    log.info("agg={}, newMsgValue={}, aggJson={}", agg, value, aggJson);
                                }
                            }

                            aggJson.put("newts", System.currentTimeMillis());
                            aggJson.put("result", resultAggJson);
                            ret = aggJson.toString();
                        }
                        catch (Exception ex) {
                            log.error("toJson Exception.", ex);
                        }

                        log.info("acc={}, value1={}, ret={}", agg, value, ret);
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