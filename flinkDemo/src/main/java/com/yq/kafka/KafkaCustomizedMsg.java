package com.yq.kafka;


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Date;
import java.util.Properties;

/**
 * className: KafkaCustomizedMsg
 *
 *  topic中输入的hello Java，  输出topic中的内容为1,hello,1552371230138l和1,java,1552371236392l
 * @author EricYang
 * @version 2019/3/11 14:50
 */
public class KafkaCustomizedMsg {
    private static final String KAFKA_BROKERS = "localhost:9092";

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(parameterTool);

        Properties properties = new Properties();
        properties.put("group.id", "flink-kafka-connector");
        properties.put("bootstrap.servers", KAFKA_BROKERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-temp", new SimpleStringSchema(), properties));

        DataStream<MyMessage> counts =
                messageStream.flatMap(new Tokenizer());

       FlinkKafkaProducer<MyMessage> myProducer = new FlinkKafkaProducer<MyMessage>(
                KAFKA_BROKERS,
                "topic1",
                new MyMessageSchema());

        myProducer.setWriteTimestampToKafka(true);
        counts.addSink(myProducer);


        // emit result
        if (parameterTool.has("output")) {
            counts.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
        }

        env.execute("Streaming Kafka");
    }

    /**
     *
     */
    public static final class Tokenizer implements FlatMapFunction<String, MyMessage> {

        @Override
        public void flatMap(String value, Collector<MyMessage> out) {
            // normalize and split the line
            String[] tokens = value.toLowerCase().split("\\W+");

            // emit the pairs
            for (String token : tokens) {
                if (token.length() > 0) {
                    out.collect(new MyMessage(1,token, new Date()));
                }
            }
        }
    }

}
