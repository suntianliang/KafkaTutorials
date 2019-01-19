package com.yq;

import kafka.admin.AdminClient;
import kafka.coordinator.group.GroupOverview;
import lombok.extern.slf4j.Slf4j;
//import org.apache.kafka.clients.CommonClientConfigs;
//import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import scala.Option;
import scala.collection.immutable.List;
import scala.concurrent.JavaConversions;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import java.util.Properties;

import static scala.concurrent.JavaConversions.*;

/**
 * Simple to Introduction
 * className: SendMessageMain
 *
 * @author EricYang
 * @version 2018/7/10 11:30
 */
@Slf4j
public class AdminMain {


    private static final String SERVERS = "ubuntu:9092";
    private static final String GROUP_ID = "yq-consumer09";

//    public static void main(String... args) throws Exception {
//
//        Properties props = new Properties();
//        //props.put("bootstrap.servers", "yamgqian-PC:9092");
//        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
//        //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "ubuntu:9092");
//
//        AdminClient adminClient = AdminClient.create(props);
//        //adminClient.ConsumerGroupSummary();
//        scala.collection.immutable.Map<TopicPartition, Object> map = adminClient.listGroupOffsets("yq-consumer03");
//        scala.collection.immutable.List<GroupOverview> groupOverviewList = adminClient.listAllConsumerGroupsFlattened();
//
//        //adminClient.listGroupOffsets("yq-consumer03");
//
//        AdminClient.ConsumerGroupSummary consumerGroupSummary = adminClient.describeConsumerGroup("yq-consumer03", 5000);
//
//        Option<scala.collection.immutable.List<AdminClient.ConsumerSummary>> summaries = consumerGroupSummary.consumers();
//        if (!summaries.isEmpty()) {
//            scala.collection.immutable.List<AdminClient.ConsumerSummary> summariesList = summaries.get();
//            AdminClient.ConsumerSummary summary = summariesList.last();
//
//            scala.collection.immutable.List<TopicPartition> partitionList = summary.assignment();
//            TopicPartition topicPartition = partitionList.last();
//            //topicPartition.
//        }
//        Collection<String> topicList = new ArrayList<>();
//        topicList.add("iot.schedulerlog.monitor");
//
//        //<TopicPartition, Object> offsets = adminClient.adminClient.listGroupOffsets("groupID"));
//        //Long offset = (Long) offsets.get(new TopicPartition("topic", 0));
//    }

    public static long getLogEndOffset(TopicPartition topicPartition){
        KafkaConsumer<String, String> consumer = getNewConsumer();
        consumer.assign(Arrays.asList(topicPartition));
        consumer.seekToEnd(Arrays.asList(topicPartition));
        long endOffset = consumer.position(topicPartition);
        return endOffset;
    }

    public static KafkaConsumer getNewConsumer(){
        Properties props = new Properties();
        props.put("bootstrap.servers", SERVERS);
        props.put("group.id", "test001");
        props.put("enable.auto.commit", "true");
        props.put("auto.offset.reset", "earliest");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }

    public static void main(String[] args) throws InstantiationException, IllegalAccessException {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        AdminClient adminClient = AdminClient.create(props);

        AdminClient.ConsumerGroupSummary  consumerGroupSummary =  adminClient.describeConsumerGroup(GROUP_ID, 5000);
        if(consumerGroupSummary.state().equals("Empty")){
            System.out.println("No grp summary");
        }
        System.out.println("consumerGrpSummary State " +  consumerGroupSummary.state());
        Option<List<AdminClient.ConsumerSummary>> consumerSummaryOption =  consumerGroupSummary.consumers();

        List<AdminClient.ConsumerSummary> ConsumerSummarys = consumerSummaryOption.get();

        scala.collection.immutable.Map<TopicPartition, Object> maps =  adminClient.listGroupOffsets(GROUP_ID);
        scala.collection.Set<TopicPartition> topicPartitions = maps.keySet();
        scala.collection.immutable.List<TopicPartition> topicPartitionList = topicPartitions.reversed();
        for(int j =0; j< topicPartitionList.size(); j++){
            TopicPartition topicPartition = topicPartitionList.apply(j);

            String currentOffset = maps.get(topicPartition).get().toString();
            long groupLastEndOffset = getLogEndOffset(topicPartition);
            long lag =  groupLastEndOffset -Long.valueOf(currentOffset);
            System.out.println("topic："+topicPartition.topic()+",  partition:" + topicPartition.partition() + ", offset："
                    +currentOffset+ ", groupLastEndOffset："+ groupLastEndOffset + ", lag:"+ lag);
        }
    }
}
