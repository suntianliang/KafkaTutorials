package com.yq.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.common.TopicPartitionInfo;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "yq-consumer11");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 20);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        System.out.println("create KafkaConsumer");

        System.out.println("receive data");
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        AdminClient adminClient = AdminClient.create(props);
        String topic = "topic01";
        adminClient.describeTopics(Arrays.asList(topic));
        try {
            DescribeTopicsResult topicResult = adminClient.describeTopics(Arrays.asList(topic));
            Map<String, KafkaFuture<TopicDescription>> descMap = topicResult.values();
            Iterator<Map.Entry<String, KafkaFuture<TopicDescription>>> itr = descMap.entrySet().iterator();
            while(itr.hasNext()) {
                Map.Entry<String, KafkaFuture<TopicDescription>> entry = itr.next();
                System.out.println("key: " + entry.getKey());
                List<TopicPartitionInfo> topicPartitionInfoList = entry.getValue().get().partitions();
                topicPartitionInfoList.forEach((e) -> {
                    int partitionId = e.partition();
                    Node node  = e.leader();
                    TopicPartition topicPartition = new TopicPartition(topic, partitionId);
                    Map<TopicPartition, Long> mapBeginning = consumer.beginningOffsets(Arrays.asList(topicPartition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr2 = mapBeginning.entrySet().iterator();
                    while(itr2.hasNext()) {
                        Map.Entry<TopicPartition, Long> tmpEntry = itr2.next();
                        System.out.println("beginningOffset of partitionId: " + tmpEntry.getKey().partition() + "  is " + tmpEntry.getValue());
                    }
                    Map<TopicPartition, Long> mapEnd = consumer.endOffsets(Arrays.asList(topicPartition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr3 = mapEnd.entrySet().iterator();
                    long lastOffset = 0;
                    while(itr3.hasNext()) {
                        Map.Entry<TopicPartition, Long> tmpEntry2 = itr3.next();
                        lastOffset = tmpEntry2.getValue();
                        System.out.println("endOffset of partitionId: " + tmpEntry2.getKey().partition() + "  is " + lastOffset);
                    }
                    long expectedOffSet = lastOffset - 1 - COUNT;
                    expectedOffSet = expectedOffSet > 0? expectedOffSet : 1;
                    System.out.println("Leader of partitionId: " + partitionId + "  is " + node + ".  expectedOffSet:"+ expectedOffSet);
                    consumer.commitSync(Collections.singletonMap(topicPartition, new OffsetAndMetadata(expectedOffSet -1 )));

                });
            }
            //topicResult.

            consumer.subscribe(Arrays.asList(topic));
            boolean setOffset = false;
            Map<String, Long> map = new HashMap<>();

            /*while(!setOffset) {
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
                    Map<TopicPartition, Long> mapBeginning = consumer.beginningOffsets(Arrays.asList(partition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr2 = mapBeginning.entrySet().iterator();
                    while(itr2.hasNext()) {
                        Map.Entry<TopicPartition, Long> entry = itr2.next();
                        System.out.println("beginningOffset of partitionId: " + entry.getKey().partition() + "  is " + entry.getValue());
                    }
                    Map<TopicPartition, Long> mapEnd = consumer.endOffsets(Arrays.asList(partition));
                    Iterator<Map.Entry<TopicPartition, Long>> itr3 = mapEnd.entrySet().iterator();
                    while(itr3.hasNext()) {
                        Map.Entry<TopicPartition, Long> entry = itr3.next();
                        System.out.println("endOffset of partitionId: " + entry.getKey().partition() + "  is " + entry.getValue());
                    }

                    log.info("partitionId={}, position={}", partition.partition(), position);
                    List<ConsumerRecord<String, String>> partitionRecords = firstRecords.records(partition);

                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    long expectedOffSet = lastOffset - 1 - COUNT;
                    expectedOffSet = expectedOffSet > 0? expectedOffSet : 1;
                    int partitionId = partition.partition();
                    System.out.printf("currentOffset=%s for positionId=%s \n", expectedOffSet,  partitionId);
                    Long existingOffset = map.get(partitionId);
                    if (existingOffset == null) {
                        map.put(String.valueOf(partitionId), expectedOffSet);
                        System.out.printf("ActualCommitOffset=%s for positionId=%s \n", expectedOffSet,  partitionId);
                        consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(expectedOffSet -1 )));
                    }
                    else {
                        if (expectedOffSet < existingOffset) {
                            map.put(String.valueOf(partitionId), expectedOffSet);
                            System.out.printf("ActualCommitOffset=%s for positionId=%s \n", expectedOffSet,  partitionId);
                            consumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(expectedOffSet -1 )));
                        }
                        else {
                            System.out.printf("existingOffset=%s, expectedOffSet=%s for positionId=%s \n", existingOffset, expectedOffSet, partitionId);
                        }
                    }

                    setOffset = true;
                }
            }*/

            //consumer.close();
            //props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
            //consumer = new KafkaConsumer<>(props);
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