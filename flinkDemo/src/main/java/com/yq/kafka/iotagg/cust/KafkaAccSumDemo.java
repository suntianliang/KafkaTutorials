package com.yq.kafka.iotagg.cust;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 *  输入消息格式为
 *  {"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":1, "T1032":2}, "ts":234843}
 *  {"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":22, "T1032":11}, "ts":234843}
 *  输出结果为 {"T1031":23}
 *            {"T1032":13}
 *  严格按照key计算消息个数
 * @author EricYang
 * @version 2019/4/28 19:16
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.yq.streaming.WordCount;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.Properties;
import java.util.Set;

@Slf4j
public class KafkaAccSumDemo {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source topic
        String sourceTopic = "acc.in.a2";
        // Sink topic
        String sinkTopic = "acc.out.a2";

        // 属性参数 - 实际投产可以在命令行传入
        Properties properties = parameterTool.getProperties();
        properties.putAll(parameterTool.getProperties());
        properties.put("bootstrap.servers", KAFKA_BROKERS);
        properties.put("group.id", "flink-kafka-connector");

        env.getConfig().setGlobalJobParameters(parameterTool);

        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                sourceTopic,
                new SimpleStringSchema(),
                properties);

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);

        DataStream<Tuple2<String, Long>> windowCounts =
                // 将json格式的消息或者文本格式消息转换为二元组，例如T1031 50， T1032 60
                input.flatMap(new MsgTokenizer())
                        .keyBy(0)
                        //计数窗口
                        .countWindow(2)
                        .sum(1);
        //最后输出时， Tuple无法直接输出，将其转换为json格式的String
        DataStream<String> resultWindows = windowCounts.map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> value) throws Exception {
                JSONObject jsonObj = new JSONObject();
                jsonObj.put(value.f0, value.f1);

                return jsonObj.toJSONString();
            }
        });

        // 创建生产者
        FlinkKafkaProducer myProducer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                properties,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        resultWindows.addSink(myProducer);

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

    /**
     *
     */
    public static final class MsgTokenizer implements FlatMapFunction<String, Tuple2<String, Long>> {

        @Override
        public void flatMap(String newMsgValue, Collector<Tuple2<String, Long>> out) {
            JSONObject newMsgJson = JSON.parseObject(newMsgValue);
            //{"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":35, "T1032":55}, "ts":234843}
            //{"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "result":{"min_T1031":35, "max_T1031":35, "max_T1032":55, "max_T1032":55}, "ts":234843}
            JSONObject dataJson = newMsgJson.getJSONObject("data");
            Set<String> keySet = dataJson.keySet();

            // emit the pairs
            for (String key : keySet) {
                    out.collect(new Tuple2<>(key, dataJson.getLong(key)));
            }
        }
    }
}

/*
  windowCounts直接sink使用SimpleStringSchema时， 出现如下错误，原因是Tuple2<String, Long> 无法作为String进行输出
Caused by: java.lang.ClassCastException: org.apache.flink.api.java.tuple.Tuple2 cannot be cast to java.lang.String
	at org.apache.flink.api.common.serialization.SimpleStringSchema.serialize(SimpleStringSchema.java:36)
 */