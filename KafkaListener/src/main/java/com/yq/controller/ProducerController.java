package com.yq.controller;

import com.alibaba.fastjson.JSONObject;
import com.yq.domain.DeviceMessage;
import com.yq.service.ProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;

import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.time.LocalDateTime;

/**
 * Simple to Introduction
 * className: ProducerController
 *
 * @author yqbjtu
 * @version 2018/4/27 8:57
 */
@RestController
@RequestMapping(value = "/producer")
public class ProducerController {
    @Autowired
    ProducerService producerService;

    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);


    @ApiOperation(value = "get")
    @GetMapping(value = "/send", produces = "application/json;charset=UTF-8")
    public String sendMsg(HttpServletResponse response) throws IOException {

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("currentTime", LocalDateTime.now().toString());

        Cookie cookie1 = new Cookie("fullname", URLEncoder.encode("name1", "UTF-8"));
        Cookie cookie2 = new Cookie("secret", "secret");


        response.addCookie(cookie1);
        response.addCookie(cookie2);

        return jsonObj.toJSONString();
    }

    @ApiOperation(value = "devMsg")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topic", value = "topic", defaultValue = "topic1", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "发送多少遍", defaultValue = "1", required = true, dataType = "int", paramType = "query"),
            @ApiImplicitParam(name = "deviceId", value = "deviceId", defaultValue = "86b874260d224cf8870bef1df60bcfff",  required = true, dataType = "string", paramType = "query")
    })
    @PostMapping(value = "/devMsg", produces = "application/json;charset=UTF-8")
    public String createDeviceMsg(@RequestParam  String topic,  @RequestParam int count, @RequestParam String deviceId) {
        producerService.send(topic, deviceId, count);
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("currentTime", LocalDateTime.now().toString());
        return jsonObj.toJSONString();
    }

    @ApiOperation(value = "devJsonMsg")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topic", value = "topic", defaultValue = "prod1", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "count", value = "发送多少遍", defaultValue = "1", required = true, dataType = "int", paramType = "query"),
            @ApiImplicitParam(name = "jsonStr", value = "jsonStr", defaultValue = "jsonStr",  required = true, dataType = "DeviceMessage", paramType = "body")
    })
    @PostMapping(value = "/devJsonMsg", produces = "application/json;charset=UTF-8")
    public String createDeviceJsonMsg(@RequestParam String topic, @RequestParam int count, @RequestBody String jsonStr) {
        producerService.sendJson(topic, jsonStr, count);
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("currentTime", LocalDateTime.now().toString());
        return jsonObj.toJSONString();
    }

    @ApiOperation(value = "notUsed")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "topic", value = "topic", defaultValue = "topic02", required = true, dataType = "string", paramType = "query"),
            @ApiImplicitParam(name = "json", value = "deviceId", defaultValue = "",  required = true, dataType = "DeviceMessage", paramType = "body")
    })
    @PostMapping(value = "/notUsed", produces = "application/json;charset=UTF-8")
    public String notUsed(@RequestParam  String topic, @RequestBody DeviceMessage msg) {
        JSONObject json = new JSONObject();
        json.put("result", "not used method");
        return json.toString();
    }

    @ApiOperation(value = "hello")
    @ApiImplicitParams({
            @ApiImplicitParam(name = "message", value = "hello", required = true, dataType = "string", paramType = "query")
    })
    @GetMapping(value = "/hello", produces = "application/json;charset=UTF-8")
    public String helloMsg(@RequestParam  String message) {
        logger.info("enter sendMsg, message={}", message);

        JSONObject jsonObj = new JSONObject();
        jsonObj.put("currentTime", LocalDateTime.now().toString());
        jsonObj.put("message", message);
        return jsonObj.toJSONString();
    }
}
