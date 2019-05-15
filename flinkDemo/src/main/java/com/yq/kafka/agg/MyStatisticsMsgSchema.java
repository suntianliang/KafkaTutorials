package com.yq.kafka.agg;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;

import java.io.IOException;

/**
 * Simple to Introduction
 * className: MyStatisticsMsgSchema
 *
 * @author EricYang
 * @version 2019/3/12 14:05
 */
public class MyStatisticsMsgSchema implements DeserializationSchema<MyStatisticsMsg>, SerializationSchema<MyStatisticsMsg> {

    @Override
    public MyStatisticsMsg deserialize(byte[] bytes) throws IOException {
        return MyStatisticsMsg.fromString(new String(bytes));
    }

    @Override
    public byte[] serialize(MyStatisticsMsg myStatisticsMsg) {
        return myStatisticsMsg.toString().getBytes();
    }

    @Override
    public TypeInformation<MyStatisticsMsg> getProducedType() {
        return TypeExtractor.getForClass(MyStatisticsMsg.class);
    }

    // Method to decide whether the element signals the end of the stream.
    // If true is returned the element won't be emitted.
    @Override
    public boolean isEndOfStream(MyStatisticsMsg myStatisticsMsg) {
        return false;
    }
}
