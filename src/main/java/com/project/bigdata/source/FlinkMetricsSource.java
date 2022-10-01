package com.project.bigdata.source;

import com.alibaba.fastjson.JSONObject;
import com.project.bigdata.entity.YarnApplicationInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.util.EnumSet;
import java.util.List;

public class FlinkMetricsSource extends RichSourceFunction<YarnApplicationInfo> {
    YarnClient yarnClient;

    @Override
    public void open(Configuration parameters) throws Exception {
        yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        yarnClient.init(conf);
        yarnClient.start();
    }

    @Override
    public void run(SourceContext<YarnApplicationInfo> context) throws Exception {
        while (true) {
            EnumSet<YarnApplicationState> appStates = EnumSet.noneOf(YarnApplicationState.class);
            appStates.add(YarnApplicationState.RUNNING);
            List<ApplicationReport> apps = yarnClient.getApplications(appStates);

            for (ApplicationReport app : apps) {
                String applicationId = app.getApplicationId().toString();
                String applicationType = app.getApplicationType();
                String userName = app.getUser();
                String appName = app.getName();
                String queue = app.getQueue();
                Resource usedResources = app.getApplicationResourceUsageReport().getUsedResources();
                long usedMemory = usedResources.getMemorySize();
                int usedVCores = usedResources.getVirtualCores();
                long startTime = app.getStartTime();
                long finishTime = app.getFinishTime();
                String applicationState = app.getYarnApplicationState().toString();
                String trackingUrl = app.getTrackingUrl();
                context.collect(new YarnApplicationInfo(applicationId, applicationType, userName, appName, queue, usedMemory, usedVCores, startTime, finishTime, applicationState, trackingUrl));
            }

            Thread.sleep(15 * 1000);
        }

    }

    @Override
    public void cancel() {

    }
}
