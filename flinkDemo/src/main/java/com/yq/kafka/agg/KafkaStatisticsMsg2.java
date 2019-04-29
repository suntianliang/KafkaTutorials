package com.yq.kafka.agg;


import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Date;
import java.util.Properties;

/**
 *
 *  className: KafkaSensorMsg
 *  Windows上  bin>flink run ..\example\streaming\xxxx
 *  本例子在KafkaStatisticsMsg基础上在map中将原始的String消息转换为Long供下游处理，
 *  可以直接在IDEA中运行，kafka的topic输入内容为20， 30之类的纯数字。
 * @author EricYang
 * @version 2019/3/11 14:50
 */
public class KafkaStatisticsMsg2 {
    private static final String KAFKA_BROKERS = "localhost:9092";
    private static final long WINDOW_SIZE = 60L;

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

        DataStream<MyStatisticsMsg> windowCounts = text.map(new MapFunction<String, Long>() {
            private static final long serialVersionUID = -6867736771747690202L;

            @Override
            public Long map(String value) throws Exception {
                System.out.println("kafka msg=" + value);
                long currV = Long.valueOf(value);
                //此处可以将topic的原始消息进行处理, 例如我们将原始数据处理为数字
                return  currV;
            }
        })
         //这里使用时间窗口，也可以选择使用countWindowAll改成计数窗口
        .timeWindowAll(Time.seconds(WINDOW_SIZE))
        .fold(new MyStatisticsMsg(0L, 0L, new Date()), new FoldFunction<Long, MyStatisticsMsg>() {
            @Override
            public MyStatisticsMsg fold(MyStatisticsMsg acc, Long value) {
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
