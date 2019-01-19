package com.yq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * 本例子试着读取当前topci最新的30条消息(如果topic内有没有30条，就获取实际消息)
 * className: SendMessageMain
 *
 * @author EricYang
 * @version 2018/7/10 11:30
 */
@Slf4j
public class ReceiveLatestMessageMain {
    private static final int COUNT = 30;
    public static void main(String... args) throws Exception {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        //每次必须换一个组，并且
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yq-consumer06");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        System.out.println("create KafkaConsumer");

        System.out.println("receive data");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        try {
            String topic = "topic01";
            consumer.subscribe(Arrays.asList(topic));
            boolean setOffset = false;
            Map<String, Long> map = new HashMap<>();

            while(!setOffset) {
                long pollStartTime = System.currentTimeMillis();
                System.out.println("start to pool. " + pollStartTime);
                ConsumerRecords<String, String> firstRecords = consumer.poll(1000);
                long pollEndTime =  System.currentTimeMillis();
                System.out.println("end to pool. diff:" + (pollEndTime - pollStartTime));
                for (ConsumerRecord<String, String> record : firstRecords) {
                    System.out.printf("set offset =%d, key=%s , value= %s, partition=%s\n",
                            record.offset(), record.key(), record.value(), record.partition());
                }
                for (TopicPartition partition : firstRecords.partitions()) {
                    long position = consumer.position(partition);

                    log.info("partitionId={}, position={}", partition.partition(), position);
                    List<ConsumerRecord<String, String>> partitionRecords = firstRecords.records(partition);

                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    long expectedOffSet = lastOffset - 1 - COUNT;
                    expectedOffSet = expectedOffSet > 0? expectedOffSet : 1;
                    int positionId = partition.partition();
                    System.out.printf("currentOffset=%s for positionId=%s \n", expectedOffSet,  positionId);
                    Long existingOffset = map.get(positionId);
                    if (existingOffset == null) {
                        map.put(String.valueOf(positionId), expectedOffSet);
                        System.out.printf("ActualCommitOffset=%s for positionId=%s \n", expectedOffSet,  positionId);
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(expectedOffSet -1 )));
                    }
                    else {
                        if (expectedOffSet < existingOffset) {
                            map.put(String.valueOf(positionId), expectedOffSet);
                            System.out.printf("ActualCommitOffset=%s for positionId=%s \n", expectedOffSet,  positionId);
                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(expectedOffSet -1 )));
                        }
                        else {
                            System.out.printf("existingOffset=%s, expectedOffSet=%s for positionId=%s \n", existingOffset, expectedOffSet,  positionId);
                        }
                    }

                    setOffset = true;
                }
            }

            consumer.close();
            //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("read offset =%d, key=%s , value= %s, partition=%s\n",
                            record.offset(), record.key(), record.value(), record.partition());
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            System.out.println("when calling kafka output error." + ex.getMessage());
        } finally {
            consumer.close();
        }
    }

}

/*
 The default is “latest,” which means that lacking a valid offset,
 the consumer will start reading from the newest records (records that were written after the consumer started running).
 The alternative is “earliest,” which means that lacking a valid offset, the consumer will read all the data in the partition,
 starting from the very beginning.
 */