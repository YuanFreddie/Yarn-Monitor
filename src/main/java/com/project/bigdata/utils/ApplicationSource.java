package com.project.bigdata.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;

/**
 * 查询yarn任务资源信息(每隔一分钟获取)
 */
public class ApplicationSource extends RichSourceFunction<JSONObject> {
    YarnClient yarnClient;

    @Override
    public void open(Configuration parameters) {
        yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        yarnClient.init(conf);
        yarnClient.start();
    }

    @Override
    public void run(SourceContext<JSONObject> out) throws Exception {

        while (true) {
            List<ApplicationReport> apps = yarnClient.getApplications();
            for (ApplicationReport app : apps) {
                HashMap<String, Object> map = new HashMap<>();
                String applicationId = app.getApplicationId().toString();
                map.put("applicationID", applicationId);
                String queue = app.getQueue();
                map.put("queue", queue);
                String user = app.getUser();
                map.put("user", user);
                String name = app.getName();
                map.put("name", name);
                String applicationType = app.getApplicationType();
                map.put("applicationType", applicationType);
                long startTime = app.getStartTime();
                map.put("startTime", startTime);
                long launchTime = app.getLaunchTime();
                map.put("launchTime", launchTime);
                long submitTime = app.getSubmitTime();
                map.put("submitTime", submitTime);
                long finishTime = app.getFinishTime();
                map.put("finishTime", finishTime);
                String curStatus = app.getYarnApplicationState().toString();
                map.put("curStatus", curStatus);
                String finalStatus = app.getFinalApplicationStatus().toString();
                map.put("finalStatus", finalStatus);
                ApplicationResourceUsageReport resource = app.getApplicationResourceUsageReport();
                Resource usedResources = resource.getUsedResources();
                long usedMemory = usedResources.getMemorySize();
                map.put("usedMemory", usedMemory);
                int usedCores = usedResources.getVirtualCores();
                map.put("usedCores", usedCores);
                long vcoreSeconds = resource.getVcoreSeconds();
                map.put("vcoreSeconds", vcoreSeconds);
                long memorySeconds = resource.getMemorySeconds();
                map.put("memorySeconds", memorySeconds);
                out.collect(new JSONObject(map));
            }
            Thread.sleep(30 * 1000);
        }
    }

    @Override
    public void cancel() {
        if (yarnClient != null) {
            try {
                yarnClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
