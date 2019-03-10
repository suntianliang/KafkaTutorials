package com.yq.join;


import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.WindowedDeserializer;
import org.apache.kafka.streams.kstream.internals.WindowedSerializer;


import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/*
* 参考  https://github.com/apache/kafka/blob/2.1/streams/examples/src/main/java/org/apache/kafka/streams/examples/pageview/PageViewUntypedDemo.java
*  can't work now
*/
public class StreamJoinDemo {

    private static final int WINDOW_SIZE = 90;

    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-key-join");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.STATE_DIR_CONFIG, System.getProperty("java.io.tmpdir"));

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Long> left = builder.stream("demo-left");
        KStream<String, Long> right = builder.stream("demo-right");
        //KStream是一个由键值对构成的抽象记录流，每个键值对是一个独立的单元，即使相同的Key也不会覆盖，类似数据库的插入操作

        KStream<String, String> joined = left.join(right,
                new ValueJoiner<Long, Long, String>() {
                    @Override
                    public String apply(Long leftValue, Long rightValue) {
                        System.out.println("leftValue:" + leftValue + ",  rightValue:" + rightValue);
                        return "left=" + leftValue + ", right=" + rightValue;
                    }
                },
                JoinWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SIZE)),
                Joined.with(
                        Serdes.String(), /* key */
                        Serdes.Long(),   /* left value */
                        Serdes.Long())  /* right value */)
                .map((user, viewRegion) -> {
                    System.out.println("user:" + user + ",  viewRegion:" + viewRegion);
                    return new KeyValue<>("key", "key");
                })
                .groupByKey(Serialized.with(Serdes.String(), Serdes.String()))
                .windowedBy(TimeWindows.of(TimeUnit.SECONDS.toMillis(WINDOW_SIZE)))
                .count()
                .toStream()
                .map((key, value) -> {
                    System.out.println("key:" + key + ",  value:" + value);
                    return new KeyValue<>("key", "key");
                });


        WindowedSerializer<String> windowedSerializer = new WindowedSerializer<>(Serdes.String().serializer());
        WindowedDeserializer<String> windowedDeserializer = new WindowedDeserializer<>(Serdes.String().deserializer(), WINDOW_SIZE);
        Serde<Windowed<String>> windowedSerde = Serdes.serdeFrom(windowedSerializer, windowedDeserializer);

        // need to override key serde to Windowed<String> type
        joined.to("demo-join", Produced.with( Serdes.String(),  Serdes.String()));
        //Produced<K, V>

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-word-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}