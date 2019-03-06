

E:\software\kafka_2.12-1.0.0\bin\windows

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