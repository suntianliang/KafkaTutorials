package com.yq.job;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Simple to Introduction
 * className: QueryJobConfig
 *
 * @author EricYang
 * @version 2019/5/15 13:39
 */
@Slf4j
public class QueryJobConfig {
    private static final String FLINK_BROKERS_URL = "http://127.0.0.1:8081";
    private static final String RUNNING = "RUNNING";
    public static void main(String[] args) throws Exception {
       //query all jobs, then save them to a map (jobName, jobId)
        QueryJobConfig queryJobConfig = new QueryJobConfig();
        Map<String, String> jobNameJobIdMap = null;

        Map<String, String> jobIdJobCfgMap = new HashMap<>();
        Map<String, String> jobIdJobNameMap = new HashMap<>();
        CloseableHttpClient httpclient = HttpClients.createDefault();

        jobNameJobIdMap = queryJobConfig.getAllRunningJobs(httpclient);
        //https://ci.apache.org/projects/flink/flink-docs-release-1.8/monitoring/rest_api.html#api
        //job query get, http://127.0.0.1:8081/jobs/c57195ef9290fa8dc977814e3af05cf7/config   /jobs/:jobid
        //response {"jid":"c57195ef9290fa8dc977814e3af05cf7","name":"remoteJar_a1b2c3","execution-con....

        //a map (jobName, jobId)
        log.info("---running job info start---");
        jobNameJobIdMap.forEach(new BiConsumer<String, String>() {
            @Override
            public void accept(String jobName, String jobId) {
                String jobCfg = queryJobConfig.getJobConfig(httpclient, jobId);
                jobIdJobCfgMap.put(jobId, jobCfg);
                jobIdJobNameMap.put(jobId, jobName);
            }
        });
        log.info("---running job info end---");

        log.info("---job cfg info start---");
        jobIdJobCfgMap.forEach(new BiConsumer<String, String>() {
            @Override
            public void accept(String jobId, String jobCfg) {
                String jobName = jobIdJobNameMap.get(jobId);
                log.info("jobId={}, jobName={}, jobCfg={}", jobId, jobName, jobCfg);
            }
        });
        log.info("---job cfg info end---");

        if (httpclient != null) {
            try {
                httpclient.close();
            }
            catch (Exception ex) {
                log.error("when closing httpclient error.", ex);
            }
        }

    }

    private Map<String, String> getAllRunningJobs(CloseableHttpClient httpclient) {
        //query all jobs, then save them to a map (jobName, jobId)
        Map<String, String> jobNameJobIdMap = new HashMap<>();
        try {
            String queryAllJobUrl = FLINK_BROKERS_URL + "/jobs/overview";
            HttpGet httpGet = new HttpGet(queryAllJobUrl);
            httpGet.setHeader("Content-Type", "application/json; charset=UTF-8");
            httpGet.setHeader("Accept", "application/json");

            CloseableHttpResponse response = httpclient.execute(httpGet);
            /*
            {
                "jobs": [
                 {
                    "jid": "403f569480137f06d23027efc8a97430",
                        "name": "remoteJar_76eb1f06587d4384a9d1dfcf4d3b7617",
                        "state": "RUNNING",
                        "start-time": 1557812496856,
                        "end-time": -1,
                        "duration": 90926542,
                        "last-modification": 1557812497253,
                        "tasks": {
                            "total": 2,
                            "created": 0,
                            "scheduled": 0,
                            "deploying": 0,
                            "running": 2,
                            "finished": 0,
                            "canceling": 0,
                            "canceled": 0,
                            "failed": 0,
                            "reconciling": 0
                        }
                }
               ]
            }*/
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                org.apache.http.HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try {
                        String jobsResult = EntityUtils.toString(entity, "utf-8");
                        //submitResult={"jobid":"944fdf3525d1f8d31b8794093514553d"}
                        JSONObject jobsJson = JSON.parseObject(jobsResult);
                        JSONArray jobJsonJSONArray = jobsJson.getJSONArray("jobs");
                        int jobSize = jobJsonJSONArray.size();
                        for(int i=0; i < jobSize; i++) {
                            JSONObject jobJsonObj = jobJsonJSONArray.getJSONObject(i);
                            String state = jobJsonObj.getString("state");
                            String jobId = jobJsonObj.getString("jid");
                            String jobName = jobJsonObj.getString("name");
                            if (RUNNING.equalsIgnoreCase(state)) {
                                jobNameJobIdMap.put(jobName, jobId);
                                log.warn("jobName={}, jobId={} is in running state. put it in map", jobName, jobId);
                            }
                            else {
                                log.warn("jobName={}, jobId={} is not in running state.", jobName, jobId);
                            }
                        }
                        log.info("running jobNameJobIdMap={}", jobNameJobIdMap);
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
            // do not close http client for we will get each job config
        }

        return jobNameJobIdMap;
    }

    private String getJobConfig(CloseableHttpClient httpclient, String jobId) {
        String queryJobCfgUrl = FLINK_BROKERS_URL + "/jobs/" + jobId + "/config";
        HttpGet httpGet = new HttpGet(queryJobCfgUrl);
        httpGet.setHeader("Content-Type", "application/json; charset=UTF-8");
        httpGet.setHeader("Accept", "application/json");

        String cfgResult = null;
        try {
            CloseableHttpResponse response = httpclient.execute(httpGet);
            log.info("cfg query result={}", response);

            /*
            {
                "jid": "403f569480137f06d23027efc8a97430",
                "name": "remoteJar_76eb1f06587d4384a9d1dfcf4d3b7617",
                "execution-config": {
                    "execution-mode": "PIPELINED",
                    "restart-strategy": "Cluster level default restart strategy",
                    "job-parallelism": 1,
                    "object-reuse-mode": false,
                    "user-config": {
                        "limitEnabled": "3",
                        "timeLimit": "15",
                        "countLimit": "2",
                        "sourceTopic": "acc.in.76eb1f06587d4384a9d1dfcf4d3b7617",
                        "group.id": "new-engine-consumer",
                        "bootstrap.servers": "10.76.3.66:9092,10.76.3.68:9092",
                        "nodeId": "76eb1f06587d4384a9d1dfcf4d3b7617",
                        "sinkTopic": "acc.out"
                    }
                }
            }
             */
            Map<String, String> jobNameJobIdMap = new HashMap<>();
            int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode >= 200 && statusCode < 300) {
                org.apache.http.HttpEntity entity = response.getEntity();
                if (entity != null) {
                    try {
                        String resultStr = EntityUtils.toString(entity, "utf-8");
                        //submitResult={"jobid":"944fdf3525d1f8d31b8794093514553d"}
                        JSONObject jsonObj = JSON.parseObject(resultStr);
                        JSONObject execCfgJson = jsonObj.getJSONObject("execution-config");
                        JSONObject userCfgJson = execCfgJson.getJSONObject("user-config");
                        cfgResult = userCfgJson.toJSONString();
                        log.info("jobId={}, cfgResult={}", jobId, cfgResult);
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
        } catch (Exception ex) {
            log.error("exception when http query job config", ex);
        }

        return cfgResult;
    }
}
