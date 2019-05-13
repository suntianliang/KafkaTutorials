package com.yq.kafka;


import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 *  className: KafkaConnector
 *
 *  iot-temp topic输入内容类似， hello Java、Hello Test、Hello Python, 先转换为DataStream<Tuple2<String, Integer>>
 *  然后将DataStream<Tuple2<String, Integer>>转换为DataStream<String>， 最后将结果写入到kafka中，
 *  结果为以下内容。 每一行为一条kafka消息
 *  Kafka and Flink says: (java,1)
 *  Kafka and Flink says: (hello,1)
 *  Kafka and Flink says: (hello,2)
 *  Kafka and Flink says: (test,1)
 *  Kafka and Flink says: (hello,3)
 *  Kafka and Flink says: (python,1)
 * @author EricYang
 * @version 2019/3/11 14:50
 */
public class KafkaConnector {
    private static final String KAFKA_BROKERS = "localhost:9092";

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


        DataStream<String> messageStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-temp", new SimpleStringSchema(), properties));

        /*messageStream.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                System.out.println("kafka msg=" + value);
                return "Kafka and Flink says: " + value;
            }
        });

        //如果addSink和counts.print();都有，在idea中只有sink生效了。
       FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                KAFKA_BROKERS,
                "topic1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        messageStream.addSink(myProducer);
        */

        DataStream<Tuple2<String, Integer>> counts =
                // split up the lines in pairs (2-tuples) containing: (word,1)
                messageStream.flatMap(new Tokenizer())
                        // group by the tuple field "0" and sum up tuple field "1"
                        .keyBy(0).sum(1);

        DataStream<String> countsString =
                counts.map(new MapFunction<Tuple2<String, Integer>, String>() {
                    private static final long serialVersionUID = -6867736771747690202L;

                    @Override
                    public String map(Tuple2<String, Integer> value) throws Exception {
                        System.out.println("kafka msg=" + value);
                        return "Kafka and Flink says: " + value;
                    }
                });

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                KAFKA_BROKERS,
                "topic1",
                new SimpleStringSchema());

        myProducer.setWriteTimestampToKafka(true);
        countsString.addSink(myProducer);

        // emit result
        if (parameterTool.has("output")) {
            counts.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            counts.print();
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

}
