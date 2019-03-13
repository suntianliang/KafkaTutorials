package com.yq.kafka;

import lombok.AllArgsConstructor;
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
public class MyMessage{

    private int id;
    private String payload;
    private Date timestamp;


    public static MyMessage fromString( String s ){
        String[] tokens = s.split( "," );
        if(tokens.length != 3) {
            throw new RuntimeException( "Invalid record: " + s );
        }

        try{
            MyMessage message = new MyMessage();
            message.id = Integer.parseInt(tokens[0]);
            message.payload = tokens[1];
            message.timestamp = new Date( Long.parseLong(tokens[0]));
            return message;
        }catch(NumberFormatException e){
            throw new RuntimeException("Invalid record: " + s);
        }
    }

    @Override
    public String toString(){
        return String.format("%d,%s,%dl", id, payload, timestamp.getTime());
    }
}
