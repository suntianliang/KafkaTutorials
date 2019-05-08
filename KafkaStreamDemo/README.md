

E:\software\kafka_2.12-1.0.0\bin\windows

<dependency>
    <groupId>org.apache.kafka</groupId>
    <artifactId>streams-quickstart-java</artifactId>
    <version>1.0.2</version>
</dependency>

kafka-run-class.bat org.apache.kafka.streams.examples.wordcount.WordCountDemo

{"temp":19, "humidity": 25}

Kafka Streams定义了三种窗口：

    跳跃时间窗口（hopping time window）：

大小固定，可能会重叠的窗口模型

     2.翻转时间窗口（tumbling time window）：

大小固定，不可重叠，无间隙的一类窗口模型

     3.滑动窗口（sliding window）：
大小固定并且沿着时间轴连续滑动的窗口模型，如果两条记录时间戳之差在窗口大小之内，则这两条数据记录属于同一个窗口。在Kafka流中，滑动窗口只有在join操作的时候才用到。

KTable<String, Long> table = stream.groupByKey()
                                   .count();
                                   
                                   SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
                                   SLF4J: Defaulting to no-operation (NOP) logger implementation
                                   SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
                                   aggKey:max,  newValue:10, aggKey:0
                                   aggKey:max,  newValue:8, aggKey:0
                                   aggKey:max,  newValue:7, aggKey:8
                                   aggKey:max,  newValue:10, aggKey:8
                                   aggKey:max,  newValue:6, aggKey:10
                                   aggKey:max,  newValue:9, aggKey:10
                                   aggKey:max,  newValue:{"temp":20, "humidity": 25}, aggKey:0
                                   aggKey:max,  newValue:{"temp":25, "humidity": 25}, aggKey:20
                                   aggKey:max,  newValue:{"temp":21, "humidity": 25}, aggKey:0
                                   aggKey:max,  newValue:{"temp":22, "humidity": 25}, aggKey:21
                                   aggKey:max,  newValue:{"temp":23, "humidity": 25}, aggKey:22
                                   aggKey:max,  newValue:{"temp":24, "humidity": 25}, aggKey:0
                                   aggKey:max,  newValue:{"temp":25, "humidity": 25}, aggKey:24
                                   aggKey:max,  newValue:{"temp":22, "humidity": 25}, aggKey:25
