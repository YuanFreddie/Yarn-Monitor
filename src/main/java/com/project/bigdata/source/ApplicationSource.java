package com.project.bigdata.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.IOException;
import java.util.*;

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
            EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
            appStates.add(YarnApplicationState.RUNNING);
            List<ApplicationReport> apps = yarnClient.getApplications(appStates);
            for (ApplicationReport app : apps) {
                JSONObject result = new JSONObject();
                String applicationId = app.getApplicationId().toString();
                result.put("applicationID", applicationId);
                String queue = app.getQueue();
                result.put("queue", queue);
                // 队列的信息
                QueueInfo item = yarnClient.getQueueInfo(queue);
                // 队列的每节点标签队列配置
                HashMap<String, JSONObject> queueConfInfo = new HashMap<>();
                result.put("queueConfInfo", queueConfInfo);
                // 队列当前状态
                String queueState = item.getQueueState().toString();
                result.put("queueState", queueState);
                
                // 队列的配置容量
                float capacity = item.getCapacity();
                result.put("capacity", capacity);
                // 队列的当前容量
                float currentCapacity = item.getCurrentCapacity();
                result.put("currentCapacity", currentCapacity);
                // 队列的默认节点标签表达式
                String defaultNodeLabelExpression = item.getDefaultNodeLabelExpression();
                result.put("defaultNodeLabelExpression", defaultNodeLabelExpression);
                // 队列的队列内抢占状态, 如果属性不在proto中，则返回null； 否则返回队列的队列内抢占状态
                Boolean intraQueuePreemptionDisabled = item.getIntraQueuePreemptionDisabled();
                result.put("intraQueuePreemptionDisabled", intraQueuePreemptionDisabled);
                // 队列的最大容量
                float maximumCapacity = item.getMaximumCapacity();
                result.put("maximumCapacity", maximumCapacity);
                // 队列的抢占状态.如果属性不在proto中，则返回null； 否则返回队列的抢占状态
                Boolean preemptionDisabled = item.getPreemptionDisabled();
                result.put("preemptionDisabled", preemptionDisabled);

                String user = app.getUser();
                result.put("user", user);
                String name = app.getName();
                result.put("name", name);
                String applicationType = app.getApplicationType();
                result.put("applicationType", applicationType);
                long startTime = app.getStartTime();
                result.put("startTime", startTime);
                long finishTime = app.getFinishTime();
                result.put("finishTime", finishTime);
                String curStatus = app.getYarnApplicationState().toString();
                result.put("curStatus", curStatus);
                String finalStatus = app.getFinalApplicationStatus().toString();
                result.put("finalStatus", finalStatus);
                ApplicationResourceUsageReport resource = app.getApplicationResourceUsageReport();
                Resource usedResources = resource.getUsedResources();
                long usedMemory = usedResources.getMemorySize();
                result.put("usedMemory", usedMemory);
                int usedCores = usedResources.getVirtualCores();
                result.put("usedCores", usedCores);
                long vcoreSeconds = resource.getVcoreSeconds();
                result.put("vcoreSeconds", vcoreSeconds);
                long memorySeconds = resource.getMemorySeconds();
                result.put("memorySeconds", memorySeconds);
                out.collect(result);
            }
            Thread.sleep(5 * 60 * 1000);
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
