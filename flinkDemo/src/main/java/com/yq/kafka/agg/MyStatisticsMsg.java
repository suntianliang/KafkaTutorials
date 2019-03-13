package com.yq.kafka.agg;

import com.yq.kafka.MyMessage;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * Simple to Introduction
 * className: MyMessage
 *
 * @author EricYang
 * @version 2019/3/12 14:05
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class MyStatisticsMsg{

    private long min;
    private long max;
    private Date timestamp;


    public static MyStatisticsMsg fromString(String s ){
        String[] tokens = s.split( "," );
        if(tokens.length != 3) {
            throw new RuntimeException( "Invalid record: " + s );
        }

        try{
            MyStatisticsMsg message = new MyStatisticsMsg();
            message.min = Long.parseLong(tokens[0]);
            message.max = Long.parseLong(tokens[1]);
            message.timestamp = new Date( Long.parseLong(tokens[0]));
            return message;
        }catch(NumberFormatException e){
            throw new RuntimeException("Invalid record: " + s);
        }
    }

    @Override
    public String toString(){
        return String.format("%d,%d,%dl", min, max, timestamp.getTime());
    }
}
