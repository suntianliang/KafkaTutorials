package com.yq.join;


import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
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
 *  className: KafkaJoinJsonEqualWindow
 *
 *  iot-tempA topic输入内容类似， {"deviceId":"A", "A1031":21}、
 *  iot-tempB topic输入内容类似， {"deviceId":"B","B1031":20}、 {"deviceId":"B","B1031":21}

 *  然后将DataStream<JSONObject>转换为DataStream<String>， 最后将结果写入到kafka中，
 *  结果为以下内容。 每一行为一条kafka消息
 *  kafka最终topic1中会出现
 *  {"A":{"A1031":21,"deviceId":"A"},"B":{"B1031":20,"deviceId":"B"}}
 *  {"A":{"A1031":21,"deviceId":"A"},"B":{"B1031":21,"deviceId":"B"}}

 *
 * @author EricYang
 * @version 2019/05/28 14:50
 */
@Slf4j
public class KafkaJoinJsonEqualWindow {
    private static final String KAFKA_BROKERS = "127.0.0.1:9092";

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
        DataStream<String> aStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-tempA", new SimpleStringSchema(), properties));

        DataStream<String> bStream = env.addSource(
                new FlinkKafkaConsumer<String>("iot-tempB", new SimpleStringSchema(), properties));



        DataStream<JSONObject> countsA =
                aStream.flatMap(new Tokenizer());

        DataStream<JSONObject> countsB =
                bStream.flatMap(new Tokenizer());

        DataStream<String> countsString = countsA.join(countsB)
                .where(new NameKeySelector())
                .equalTo(new EqualKeySelector())
                .window(TumblingEventTimeWindows.of(Time.minutes(2)))
                .apply (new JoinFunction<JSONObject, JSONObject, String>(){
                    @Override
                    public String join(JSONObject first, JSONObject second) {
                        JSONObject joinJson = new JSONObject();
                        joinJson.put(first.getString("deviceId"), first);
                        joinJson.put(second.getString("deviceId"), second);

                        return  joinJson.toJSONString();
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
        JobExecutionResult result = env.execute("Streaming Kafka3");
        JobID jobId = result.getJobID();
        System.out.println("jobId=" + jobId);
    }


    public static final class Tokenizer implements FlatMapFunction<String, JSONObject> {

        @Override
        public void flatMap(String value, Collector<JSONObject> out) {
            try {
                JSONObject json = JSONObject.parseObject(value);
                out.collect(json);
            } catch (Exception ex) {
                log.error("json format error. jsonStr={}", value, ex);
            }

        }
    }

    private static class NameKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            final String str = "A";
            return str;
        }
    }

    private static class EqualKeySelector implements KeySelector<JSONObject, String> {
        @Override
        public String getKey(JSONObject value) {
            final String str = "A";
            return str;
        }
    }
}
