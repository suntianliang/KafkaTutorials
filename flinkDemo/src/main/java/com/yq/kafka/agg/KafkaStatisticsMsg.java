package com.yq.kafka.agg;


import com.yq.kafka.MyMessage;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Date;
import java.util.Properties;

/**
 *  className: KafkaStatisticsMsg
 *  Windows上  bin>flink run ..\example\streaming\xxxx
 *  可以直接在IDEA中运行，kafka的topic输入内容为20， 30之类的纯数字。
 * @author EricYang
 * @version 2019/3/11 14:50
 */
public class KafkaStatisticsMsg {
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final long WINDOW_SIZE = 60L;
    private DataStream<MyStatisticsMsg> windowCounts;

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


        DataStream<String> text = env.addSource(
                new FlinkKafkaConsumer<String>("iot-temp", new SimpleStringSchema(), properties));

        DataStream<MyStatisticsMsg> windowCounts = text.map(new MapFunction<String, String>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public String map(String value) throws Exception {
                System.out.println("kafka msg=" + value);
                //此处可以将topic的原始消息进行处理， 这里不做处理只是打印
                //return "Kafka and Flink says: " + value;
                return  value;
            }
        })
        .timeWindowAll(Time.seconds(WINDOW_SIZE))
        .fold(new MyStatisticsMsg(0L, 0L, new Date()), new FoldFunction<String, MyStatisticsMsg>() {
            @Override
            public MyStatisticsMsg fold(MyStatisticsMsg acc, String value) {
                System.out.println("value1:" + value);
                long currV = Long.valueOf(value);
                if (currV > acc.getMax()) {
                    acc.setMax(currV);
                }

                if (currV < acc.getMin()) {
                    acc.setMin(currV);
                }

                return acc;
            }
        });

       FlinkKafkaProducer<MyStatisticsMsg> myProducer = new FlinkKafkaProducer<MyStatisticsMsg>(
                KAFKA_BROKERS,
                "topic1",
                new MyStatisticsMsgSchema());

        myProducer.setWriteTimestampToKafka(true);
        windowCounts.addSink(myProducer);


        // emit result
        if (parameterTool.has("output")) {
            windowCounts.writeAsText(parameterTool.get("output"));
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            windowCounts.print();
        }

        env.execute("Streaming Kafka");
    }

}
