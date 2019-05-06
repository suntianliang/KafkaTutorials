package com.yq.kafka.iotagg;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 *  {"deviceid":"xxx", "chainid":"yyyyy", "func":["min","max"], "data":{"T1031":35, "T1032":55}, "ts":234843}
 *  从agg.in.abc1读取消息后，在新消息上执行如下操作， 也就是把原有消息的ts变为oldts，将当前时间作为ts传入到json中
 *   tempAgg.put("ts", System.currentTimeMillis());
     tempAgg.put("oldts", ts);
 * @author EricYang
 * @version 2019/4/28 19:16
 */

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

import java.util.Properties;

@Slf4j
public class KafkaAggExample {
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final long WINDOW_SIZE = 60L;

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

        // 数据处理
        ObjectMapper objectMapper = new ObjectMapper();
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
                    public String fold(String acc, String value) {
                       log.info("acc={}, value1={}" ,acc, value);

                        String ret = null;
                        try {
                            JsonNode jsonNode = objectMapper.readTree(value);
                            JsonNode dataField = jsonNode.get("data");
                            if (dataField != null) {

                            }
                            long ts = jsonNode.get("ts").asLong();
                            ObjectNode tempAgg = (ObjectNode)jsonNode;
                            tempAgg.put("ts", System.currentTimeMillis());
                            tempAgg.put("oldts", ts);
                            ret = tempAgg.toString();
                        }
                        catch (Exception ex) {
                            log.error("ex",  ex);
                        }

                        log.info("acc={}, value1={}, ret={}" ,acc, value, ret);
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