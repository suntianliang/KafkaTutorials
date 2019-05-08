package com.yq;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FoldFunction;

import java.util.List;

/**
 * Simple to Introduction
 * className: MyFoldFunction
 *
 * @author EricYang
 * @version 2019/5/7 16:01
 */
@Slf4j
public class MyFoldFunction implements FoldFunction<String, String> {
    private static final String MAX_PREFIX = "MAX_";
    private static final String MIN_PREFIX = "MIN_";
    private static final String AVG_PREFIX = "AVG_";
    private static final String SUM_PREFIX = "SUM_";
    private static final String COUNT = "COUNT";

    @Override
    public String fold(String acc, String newMsgValue) {
        log.info("acc={}, newMsgValue={}", acc, newMsgValue);
        String ret = null;
        try {
            //{"deviceid":"xxx", "chainId":"yyyyy", "ruleId":"a1",
            // "cfg":{"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2},
            // "data":{"T1031":35, "T1032":55}, "ts":234843}
            JSONObject newMsgJson = JSON.parseObject(newMsgValue);
            //{"deviceid":"xxx", "chainid":"yyyyy", "ruleId":"a1",
            // "result":{"min_T1031":35, "max_T1031":35, "max_T1032":55, "max_T1032":55, "count":15,}, "ts":234843}
            JSONObject accJson = JSON.parseObject(acc);

            JSONObject dataJson = newMsgJson.getJSONObject("data");
            JSONObject cfgJson = newMsgJson.getJSONObject("cfg");
            JSONObject resultAggJson = accJson.getJSONObject("result");
            if (dataJson != null) {
                JSONArray jsonArray = cfgJson.getJSONArray("sensorCodeList");
                if (jsonArray != null) {
                    List<String> sensorCodeList = jsonArray.toJavaList(String.class);
                    Boolean calMAX = cfgJson.getBoolean("calMAX");
                    Boolean calMIN = cfgJson.getBoolean("calMIN");
                    Boolean calAVG = cfgJson.getBoolean("calAVG");
                    Boolean calSUM = cfgJson.getBoolean("calSUM");

                    if (resultAggJson != null) {
                        for (String sensorCode : sensorCodeList) {
                            //传感器数据可能不全，因此必须判断是否存在该传感器
                            if (dataJson.containsKey(sensorCode)) {
                                double newSensorValue = dataJson.getLongValue(sensorCode);
                                if (calMAX != null && calMAX) {
                                    boolean isContain = resultAggJson.containsKey(MAX_PREFIX + sensorCode);
                                    if (isContain) {
                                        double currentMaxValue = resultAggJson.getDoubleValue(MAX_PREFIX + sensorCode);
                                        if (newSensorValue > currentMaxValue) {
                                            resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                            log.info("MAX oldMax={}, newMax={}, sensorCode={}, newSensorValue={},",
                                                    currentMaxValue, newSensorValue, sensorCode, newSensorValue);
                                        } else {
                                            log.info("MAX oldMin={}, no newMax, sensorCode={}, newSensorValue={},",
                                                    currentMaxValue, sensorCode, newSensorValue);
                                        }
                                    } else {
                                        resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                        log.info("MAX no oldMax, newMax={}, sensorCode={}, newSensorValue={},",
                                                sensorCode, newSensorValue);
                                    }
                                }

                                if (calMIN != null && calMIN) {
                                    boolean isContain = resultAggJson.containsKey(MIN_PREFIX + sensorCode);
                                    if (isContain) {
                                        double currentMinValue = resultAggJson.getDoubleValue(MIN_PREFIX + sensorCode);
                                        if (newSensorValue < currentMinValue) {
                                            resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                            log.info("MIN oldMin={}, newMin={}, sensorCode={}, newSensorValue={},",
                                                    currentMinValue, newSensorValue, sensorCode, newSensorValue);
                                        } else {
                                            log.info("MIN oldMin={}, no newMin, sensorCode={}, newSensorValue={},",
                                                    currentMinValue, sensorCode, newSensorValue);
                                        }
                                    } else {
                                        resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                        log.info("MIN no oldMin, newMin={}, sensorCode={}, newSensorValue={},",
                                                sensorCode, newSensorValue);
                                    }
                                }

                                if (calSUM != null && calSUM) {
                                    double currentSumValue = resultAggJson.getDoubleValue(SUM_PREFIX + sensorCode);
                                    double newSum = currentSumValue + newSensorValue;
                                    resultAggJson.put(SUM_PREFIX + sensorCode, currentSumValue + newSensorValue);
                                    log.info("SUM oldSum={}, newSum={}, sensorCode={}, newSensorValue={},",
                                            currentSumValue, newSum, sensorCode, newSensorValue);
                                }

                                //when getting avg, the max is necessary.
                                if (calAVG != null && calAVG) {
                                    long count = resultAggJson.getLongValue(COUNT);
                                    //if the sum is not calculated, compute it now
                                    if (calSUM == null || !calSUM) {
                                        //no need to check whether sum contain key for the default is 0
                                        double currentSumValue = resultAggJson.getLongValue(SUM_PREFIX + sensorCode);
                                        double newSum = currentSumValue + newSensorValue;
                                        resultAggJson.put(SUM_PREFIX + sensorCode, currentSumValue + newSensorValue);
                                        log.info("SUM oldSum={}, newSum={}, sensorCode={}, newSensorValue={}",
                                                currentSumValue, newSum, sensorCode, newSensorValue);
                                    }

                                    //calculate the avg according to sum and count. avg should be double.
                                    double currentSumValue = resultAggJson.getDoubleValue(SUM_PREFIX + sensorCode);
                                    double currentAvgValue = resultAggJson.getDoubleValue(AVG_PREFIX + sensorCode);
                                    double newAvgValue = currentSumValue / (count + 1);
                                    resultAggJson.put(AVG_PREFIX + sensorCode, newAvgValue);
                                    log.info("AVG oldAvg={}, currentSumValue={}, count={}, newAvg={}, sensorCode={}",
                                            currentAvgValue, currentSumValue, count, newAvgValue, sensorCode);
                                }
                            }
                        }
                    } else {
                        resultAggJson = new JSONObject();
                        //如果初始化没有值就给result附上值
                        for (String sensorCode : sensorCodeList) {
                            //传感器数据可能不全，因此必须判断是否存在该传感器
                            if (dataJson.containsKey(sensorCode)) {
                                double newSensorValue = dataJson.getDoubleValue(sensorCode);
                                if (calMAX != null && calMAX) {
                                    resultAggJson.put(MAX_PREFIX + sensorCode, newSensorValue);
                                    log.info("MAX initMax={}, sensorCode={}", newSensorValue, sensorCode);
                                }

                                if (calMIN != null && calMIN) {
                                    resultAggJson.put(MIN_PREFIX + sensorCode, newSensorValue);
                                    log.info("MIN initMin={}, sensorCode={}", newSensorValue, sensorCode);
                                }

                                if (calSUM != null && calSUM) {
                                    resultAggJson.put(SUM_PREFIX + sensorCode, newSensorValue);
                                    log.info("SUM initSum={}, sensorCode={}", newSensorValue, sensorCode);
                                }

                                if (calAVG != null && calAVG) {
                                    if (calSUM == null || !calSUM) {
                                        resultAggJson.put(SUM_PREFIX + sensorCode, newSensorValue);
                                        log.info("SUM initSum={} in avg, sensorCode={}", newSensorValue, sensorCode);
                                    }
                                    resultAggJson.put(AVG_PREFIX + sensorCode, newSensorValue);
                                    log.info("AVG initAvg={}, sensorCode={}", newSensorValue, sensorCode);
                                }
                            }
                        }

                        accJson.put("chainId", newMsgJson.getString("chainId"));
                        accJson.put("deviceId", newMsgJson.getString("deviceId"));
                        accJson.put("nodeId", newMsgJson.getString("nodeId"));
                    }

                    long count = resultAggJson.getLongValue(COUNT);
                    resultAggJson.put(COUNT, count + 1);
                    resultAggJson.put("data" + count, dataJson);
                    log.info("acc={}, newMsgValue={}, resultAggJson={}", acc, newMsgValue, resultAggJson);
                }
            }

            accJson.put("newts", System.currentTimeMillis());
            accJson.put("result", resultAggJson);
            ret = accJson.toString();
        } catch (Exception ex) {
            log.error("toJson Exception.", ex);
        }

        log.info("acc={}, newMsgValue={}, ret={}", acc, newMsgValue, ret);
        return ret;
    }

}
