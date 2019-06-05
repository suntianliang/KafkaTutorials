package com.yq.join;


import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 *  className: KafkaJoinJson3Window
 *
 *  iot-tempA topic输入内容类似， {"A":{"A1031":21}}  {"B":{"B1031":21}}  {"A":{"C1031":30}}

 *  然后将DataStream<JSONObject>转换为DataStream<String>， 最后将结果写入到kafka中，
 *  结果为以下内容。 每一行为一条kafka消息
 *  kafka最终topic1中会出现
 *  Window: TimeWindow{start=1559704170294, end=1559704255873}input: [{"A":{"A1031":21}}, {"B":{"B1031":21}}, {"A":{"A1031":30}}]

 *
 * @author EricYang
 * @version 2019/05/28 14:50
 */
@Slf4j
public class KafkaJoinJson3Window {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";
    private static final long WINDOW_SIZE = 60L;

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        //createRemoteEnvironment(String host, int port, String... jarFiles)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-kafka-connector2");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //it is necessary to use IngestionTime, not EventTime. during my running this program
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        // 创建消费者
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer<String>(
                "iot-tempA",
                new SimpleStringSchema(),
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
                //.timeWindowAll(Time.seconds(WINDOW_SIZE))
                .keyBy(new NameKeySelector())
                .window(ProcessingTimeSessionWindows.withGap(Time.minutes(1)))
                .process(new MyProcessWindowFunction());


        windowCounts.print();
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                KAFKA_BROKERS,
                "topic1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        windowCounts.addSink(myProducer);

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }

    private static class NameKeySelector implements KeySelector<String, String> {
        @Override
        public String getKey(String value) {
            final String str = "A";
            return str;
        }
    }
}
