package com.yq.join;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Simple to Introduction
 * className: MyProcessWindowFunction
 *
 * @author EricYang
 * @version 2019/6/5 10:49
 */
@Slf4j
public class MyProcessWindowFunction
        extends ProcessWindowFunction<String, String, String, TimeWindow> {

    @Override
    public void process(String key, Context context, Iterable<String> input, Collector<String> out) {
        long count = 0;
        for (String in: input) {
            count++;
        }
        out.collect("Window: " + context.window() + "input: " + input);
    }
}
