package com.yq.kafka.iotagg;


import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

@Slf4j
public class KafkaSingleMsgDemo {
    private static final String KAFKA_BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Source的topic
        String sourceTopic = "flink-topic";
        // Sink的topic
        String sinkTopic = "flink-topic-output";

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
        // 设置读取最早的数据
        // consumer.setStartFromEarliest();

        // 读取Kafka消息
        DataStream<String> input = env.addSource(consumer);

        // 数据处理
        DataStream<String> result = input.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                String msg = "addPart ".concat(s);
                log.info("map_msg={}", msg);
                return msg;
            }
        });

        // 创建生产者
        FlinkKafkaProducer producer = new FlinkKafkaProducer<String>(
                sinkTopic,
                new KeyedSerializationSchemaWrapper<String>(new KafkaMsgSchema()),
                properties,
                FlinkKafkaProducer.Semantic.AT_LEAST_ONCE);

        // 将数据写入Kafka指定Topic中
        result.addSink(producer);

        // emit result
        if (parameterTool.has("output")) {
            result.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }

        // 执行job
        env.execute("Kafka Example");
    }
}