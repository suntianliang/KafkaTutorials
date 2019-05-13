package com.yq.kafka;

/**
 * Simple to Introduction
 * className: KafkaSingleMsgDemo
 * cfg
   {"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2}

 {"deviceId":"001", "chainId":"c1", "nodeId":"n1", "cfg":{"sensorCodeList": ["T1031","T1032"], "timeLimit": 2, "calMAX": false, "calMIN": true, "calAVG": true, "limitEnabled": 2},"data":{"T1031":35, "T1032":55}, "ts":234843}
 * @author EricYang
 * @version 2019/4/28 19:16
 */

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

@Slf4j
public class SubmitKafkaAccProdMain {
    private static final String KAFKA_BROKERS = "192.168.1.1:9092,192.168.1.2:9092";
    private static final String FLINK_BROKERS_URL = "http://192.168.1.3:8081";
    private static final String JAR_ID = "7f563a0f-03a3-41e7-997f-0e2bd9641f30_AccumulateNode-1.0-jar-with-dependencies.jar";
    private static final String ENTRY_CLASS = "org.iot.KafkaAccProd";

    public static void main(String[] args) throws Exception {
        // 属性参数 - 实际投产可以在命令行传入
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("--bootstrap.servers \"").append(KAFKA_BROKERS).append("\"");
        stringBuilder.append("  --group.id ").append("grp01");
        stringBuilder.append("  --limitEnabled ").append(2);
        stringBuilder.append("  --timeLimit ").append(2);
        stringBuilder.append("  --countLimit ").append(5);
        stringBuilder.append("  --nodeId ").append("remote1");
        stringBuilder.append("  --sourceTopic ").append("acc.in.remote1");
        stringBuilder.append("  --sinkTopic ").append("acc.out");


        String encodeSubmitJobUrl = FLINK_BROKERS_URL + "/jars/" + JAR_ID + "/run";
        String jobId;

        JSONObject bodyJson = new JSONObject();
        bodyJson.put("entryClass", ENTRY_CLASS);
        bodyJson.put("programArgs", stringBuilder.toString());
        String requestBody = bodyJson.toJSONString();

        CloseableHttpClient httpclient = HttpClients.createDefault();
        try {
            HttpPost httpPost = new HttpPost(encodeSubmitJobUrl);
            httpPost.setHeader("Content-Type", "application/json; charset=UTF-8");
            httpPost.setHeader("Accept", "application/json");
            StringEntity bodyEntity = new StringEntity(bodyJson.toJSONString(), ContentType.APPLICATION_JSON);
            httpPost.setEntity(bodyEntity);

            CloseableHttpResponse response = httpclient.execute(httpPost);

            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                org.apache.http.HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try {
                        String submitResult = EntityUtils.toString(entity, "utf-8");
                        //submitResult={"jobid":"944fdf3525d1f8d31b8794093514553d"}
                       JSONObject jobJson = JSON.parseObject(submitResult);
                       jobId = jobJson.getString("jobid");
                       log.info("jobId={}", jobId);
                    }
                    catch (Exception ex) {
                        log.warn("http response entity parse error.", ex);
                    }
                }
                else {
                    log.warn("http response entity no jobId.");
                }
            } else {
                log.error("error responseCode={}", statusCode);
            }

            response.close();
        } catch (Exception ex) {
            log.error("exception when http post", ex);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                }
                catch (Exception ex) {
                    log.error("when closing httpclient url output error. submitJobUrl={}", encodeSubmitJobUrl, ex);
                }
            }
        }

        //查询jar http://127.0.0.1:8081/jars/
        //response {"address":"http://localhost:8081","files":[{"id":"1b75e309-e113-4bab-a2a5-f0d92844b992_FlinkRemoteDemoJar-1.0-SNAPSHOT-jar-with-dependencies.jar",
        // "name":"FlinkRemoteDemoJar-1.0-SNAPSHOT-jar-with-dependencies.jar",
        // "uploaded":1557282678923,
        // "entry":[{"name":"KafkaAccProd",
        // "description":null}]}]}

        //Request URL: http://127.0.0.1:8081/
        //jars/1b75e309-e113-4bab-a2a5-f0d92844b992_FlinkRemoteDemoJar-1.0-SNAPSHOT-jar-with-dependencies.jar/
        // run?entry-class=KafkaAccProd&program-args=--bootstrap.servers+10.76.3.70:9092+--limitEnabled+2+--timeLimit+2+--group.id+grp01+--nodeId+a1b2c3
        //Request Method: POST
                //{"jobid":"c57195ef9290fa8dc977814e3af05cf7"}
        //https://ci.apache.org/projects/flink/flink-docs-release-1.8/monitoring/rest_api.html#api
        //job query get, http://127.0.0.1:8081/jobs/c57195ef9290fa8dc977814e3af05cf7/config   /jobs/:jobid
        //response {"jid":"c57195ef9290fa8dc977814e3af05cf7","name":"remoteJar_a1b2c3","execution-con....

        jobId = "944fdf3525d1f8d31b8794093514553d";
        //PATCH  /jobs/:jobid, terminate job
        String terminateJobUrl = FLINK_BROKERS_URL + "/jobs/" + jobId ;
        HttpPatch httpPatch = new HttpPatch(terminateJobUrl);
        httpPatch.setHeader("Content-Type", "application/json; charset=UTF-8");
        httpPatch.setHeader("Accept", "application/json");
        try {
            CloseableHttpResponse response = httpclient.execute(httpPatch);
            log.info("terminated result={}", response);
        } catch (Exception ex) {
            log.error("exception when http patch", ex);
        } finally {
            if (httpclient != null) {
                try {
                    httpclient.close();
                }
                catch (Exception ex) {
                    log.error("when closing httpclient error. terminateJobUrl={}", terminateJobUrl, ex);
                }
            }
        }
    }
}