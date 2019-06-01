package com.yq.join;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 *  className: KafkaConnector
 *
 *  iot-tempA topic输入内容类似， hello Java、先转换为DataStream<Tuple2<String, Integer>>
 *  iot-tempB topic输入内容类似， hello Java、先转换为DataStream<Tuple2<String, Integer>>
 *  然后将DataStream<Tuple2<String, Integer>>转换为DataStream<String>， 最后将结果写入到kafka中，
 *  结果为以下内容。 每一行为一条kafka消息
 *  kafka最终topic1中会出现
 *  hello(1,1)
 *  java(1,1)
 *  hello(1,1)

 * @author EricYang
 * @version 2019/05/27 14:50
 */
public class KafkaJoinWindow {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        //createRemoteEnvironment(String host, int port, String... jarFiles)
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-kafka-connector");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //it is necessary to use IngestionTime, not EventTime. during my running this program
        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
        DataStream<String> aStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-tempA", new SimpleStringSchema(), properties));

        DataStream<String> bStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-tempB", new SimpleStringSchema(), properties));


        DataStream<Tuple2<String, Integer>> countsA =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                aStream.flatMap(new Tokenizer());

        DataStream<Tuple2<String, Integer>> countsB =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                bStream.flatMap(new Tokenizer());

        DataStream<String> countsString = countsA.join(countsB)
                .where(new NameKeySelector())
                .equalTo(new NameKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .apply (new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>(){
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple2<String, Integer> second) {
                        return first.f0 + "(" + first.f1 + ","+ second.f1 + ")";
                    }
                });


        countsString.print();
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                KAFKA_BROKERS,
                "topic1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        countsString.addSink(myProducer);

        // emit result
        if (parameterTool.has("output")) {
            countsString.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            countsString.print();
        }

        // execute program
        JobExecutionResult result = env.execute("Streaming Kafka");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }

    /**
     * Implements the string tokenizer that splits sentences into words as a
     * user-defined FlatMapFunction. The function takes a line (String) and
     * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
     * Integer>}).
     */
    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new Tuple2<>(token, 1));
                }
            }
        }
    }

    private static class NameKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        @Override
        public String getKey(Tuple2<String, Integer> value) {
            return value.f0;
        }
    }
}
