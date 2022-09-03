package com.project.bigdata.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.IOException;
import java.util.*;

public class YarnQueueInfoSource extends RichSourceFunction<JSONObject> {
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
            List<QueueInfo> allQueues = yarnClient.getAllQueues();
            for (QueueInfo item : allQueues) {
                JSONObject result = new JSONObject();
                // 一级队列的名字
                String queueName = item.getQueueName();
                result.put("queueName", queueName);
                // 一级队列的每节点标签队列配置
                Map<String, QueueConfigurations> queueConf = item.getQueueConfigurations();
                HashMap<String, JSONObject> queueConfInfo = new HashMap<>();
                for (Map.Entry<String, QueueConfigurations> entry : queueConf.entrySet()) {
                    JSONObject json = new JSONObject();
                    String key = entry.getKey();
                    QueueConfigurations value = entry.getValue();
                    // 获取配置的队列容量
                    float capacity = value.getCapacity();
                    json.put("capacity", capacity);
                    // 获取配置的绝对容量
                    float absoluteCapacity = value.getAbsoluteCapacity();
                    json.put("absoluteCapacity", absoluteCapacity);
                    // 获取绝对最大容量
                    float absoluteMaxCapacity = value.getAbsoluteMaxCapacity();
                    json.put("absoluteMaxCapacity", absoluteMaxCapacity);
                    // 获取配置的队列最大容量（来自绝对资源）
                    Resource configuredMaxCapacity = value.getConfiguredMaxCapacity();
                    if (configuredMaxCapacity != null) {
                        long configuredMaxMemorySize = configuredMaxCapacity.getMemorySize();
                        json.put("configuredMaxMemorySize", configuredMaxMemorySize);
                        int configuredMaxVirtualCores = configuredMaxCapacity.getVirtualCores();
                        json.put("configuredMaxVirtualCores", configuredMaxVirtualCores);
                        List<ResourceInformation> configuredMaxAllResourcesList = configuredMaxCapacity.getAllResourcesListCopy();
                        json.put("configuredMaxAllResourcesList", configuredMaxAllResourcesList);
                    }

                    // 获取配置的队列最小容量（来自绝对资源）
                    Resource configuredMinCapacity = value.getConfiguredMinCapacity();
                    if (configuredMinCapacity != null) {
                        long configuredMinMemorySize = configuredMinCapacity.getMemorySize();
                        json.put("configuredMinMemorySize", configuredMinMemorySize);
                        int configuredMinVirtualCores = configuredMinCapacity.getVirtualCores();
                        json.put("configuredMinVirtualCores", configuredMinVirtualCores);
                        List<ResourceInformation> configuredMinAllResourcesList = configuredMinCapacity.getAllResourcesListCopy();
                        json.put("configuredMinAllResourcesList", configuredMinAllResourcesList);
                    }

                    // 获取队列的有效最大容量（来自绝对资源）
                    Resource effectiveMaxCapacity = value.getEffectiveMaxCapacity();
                    if (effectiveMaxCapacity != null) {
                        long effectiveMaxMemorySize = effectiveMaxCapacity.getMemorySize();
                        json.put("effectiveMaxMemorySize", effectiveMaxMemorySize);
                        int effectiveMaxVirtualCores = effectiveMaxCapacity.getVirtualCores();
                        json.put("effectiveMaxVirtualCores", effectiveMaxVirtualCores);
                        List<ResourceInformation> effectiveMaxAllResourcesList = effectiveMaxCapacity.getAllResourcesListCopy();
                        json.put("effectiveMaxAllResourcesList", effectiveMaxAllResourcesList);
                    }

                    // 获取队列的有效最小容量（来自绝对资源）
                    Resource effectiveMinCapacity = value.getEffectiveMinCapacity();
                    if (effectiveMinCapacity != null) {
                        long effectiveMinMMemorySize = effectiveMinCapacity.getMemorySize();
                        json.put("effectiveMinMMemorySize", effectiveMinMMemorySize);
                        int effectiveMinVirtualCores = effectiveMinCapacity.getVirtualCores();
                        json.put("effectiveMinVirtualCores", effectiveMinVirtualCores);
                        List<ResourceInformation> effectiveMinAllResourcesList = effectiveMinCapacity.getAllResourcesListCopy();
                        json.put("effectiveMinAllResourcesList", effectiveMinAllResourcesList);
                    }

                    // 获取最大的容量
                    float maxCapacity = value.getMaxCapacity();
                    json.put("maxCapacity", maxCapacity);

                    queueConfInfo.put(key, json);
                }
                result.put("queueConfInfo", queueConfInfo);
                // 一级队列当前状态
                String queueState = item.getQueueState().toString();
                result.put("queueState", queueState);
                QueueStatistics queueStatistics = item.getQueueStatistics();
                if (queueStatistics != null) {
                    // 一级队列分配的Container容器个数
                    long allocatedContainers = queueStatistics.getAllocatedContainers();
                    result.put("allocatedContainers", allocatedContainers);
                    // 一级队列被分配的内存大小(以MB为单位)
                    long allocatedMemoryMB = queueStatistics.getAllocatedMemoryMB();
                    result.put("allocatedMemoryMB", allocatedMemoryMB);
                    // 一级队列被分配的vcores
                    long allocatedVCores = queueStatistics.getAllocatedVCores();
                    result.put("allocatedVCores", allocatedVCores);
                    // 一级队列的可用内存(以MB为单位)
                    long availableMemoryMB = queueStatistics.getAvailableMemoryMB();
                    result.put("availableMemoryMB", availableMemoryMB);
                    // 一级队列的可用vcores
                    long availableVCores = queueStatistics.getAvailableVCores();
                    result.put("availableVCores", availableVCores);
                    // 一级队列的活跃用户数
                    long numActiveUsers = queueStatistics.getNumActiveUsers();
                    result.put("numActiveUsers", numActiveUsers);
                    // 一级队列已完成的app个数
                    long numAppsCompleted = queueStatistics.getNumAppsCompleted();
                    result.put("numAppsCompleted", numAppsCompleted);
                    // 一级队列失败的app个数
                    long numAppsFailed = queueStatistics.getNumAppsFailed();
                    result.put("numAppsFailed", numAppsFailed);
                    // 一级队列被kill掉的app个数
                    long numAppsKilled = queueStatistics.getNumAppsKilled();
                    result.put("numAppsKilled", numAppsKilled);
                    // 一级队列状态为pending的app个数
                    long numAppsPending = queueStatistics.getNumAppsPending();
                    result.put("numAppsPending", numAppsPending);
                    // 一级队列正在运行的app个数
                    long numAppsRunning = queueStatistics.getNumAppsRunning();
                    result.put("numAppsRunning", numAppsRunning);
                    // 一级队列提交的app数量
                    long numAppsSubmitted = queueStatistics.getNumAppsSubmitted();
                    result.put("numAppsSubmitted", numAppsSubmitted);
                    // 一级队列待处理container容器的数量
                    long pendingContainers = queueStatistics.getPendingContainers();
                    result.put("pendingContainers", pendingContainers);
                    // 一级队列待处理的内存(MB)
                    long pendingMemoryMB = queueStatistics.getPendingMemoryMB();
                    result.put("pendingMemoryMB", pendingMemoryMB);
                    // 一级队列待处理的vcores
                    long pendingVCores = queueStatistics.getPendingVCores();
                    result.put("pendingVCores", pendingVCores);
                    // 一级队列预留Container容器的数量
                    long reservedContainers = queueStatistics.getReservedContainers();
                    result.put("reservedContainers", reservedContainers);
                    // 一级队列预留的内存
                    long reservedMemoryMB = queueStatistics.getReservedMemoryMB();
                    result.put("reservedMemoryMB", reservedMemoryMB);
                    // 一级队列预留的vcores
                    long reservedVCores = queueStatistics.getReservedVCores();
                    result.put("reservedVCores", reservedVCores);
                }

                // 一级队列的可访问节点标签
                Set<String> accessibleNodeLabels = item.getAccessibleNodeLabels();
                result.put("accessibleNodeLabels", accessibleNodeLabels);
                // 一级队列的配置容量
                float capacity = item.getCapacity();
                result.put("capacity", capacity);
                // 一级队列的当前容量
                float currentCapacity = item.getCurrentCapacity();
                result.put("currentCapacity", currentCapacity);
                // 一级队列的默认节点标签表达式
                String defaultNodeLabelExpression = item.getDefaultNodeLabelExpression();
                result.put("defaultNodeLabelExpression", defaultNodeLabelExpression);
                // 一级队列的队列内抢占状态, 如果属性不在proto中，则返回null； 否则返回队列的队列内抢占状态
                Boolean intraQueuePreemptionDisabled = item.getIntraQueuePreemptionDisabled();
                result.put("intraQueuePreemptionDisabled", intraQueuePreemptionDisabled);
                // 一级队列的最大容量
                float maximumCapacity = item.getMaximumCapacity();
                result.put("maximumCapacity", maximumCapacity);
                // 一级队列的抢占状态.如果属性不在proto中，则返回null； 否则返回队列的抢占状态
                Boolean preemptionDisabled = item.getPreemptionDisabled();
                result.put("preemptionDisabled", preemptionDisabled);

                // 一级队列的所有作业信息
                List<ApplicationReport> applications = item.getApplications();
                ArrayList<JSONObject> appsList = new ArrayList<>();
                if (applications != null) {
                    System.out.println("一级队列：" + queueName + "，当前正在运行的任务个数：" + applications.size());
                    for (ApplicationReport app : applications) {
                        JSONObject jsonObject = new JSONObject();
                        ApplicationId applicationId = app.getApplicationId();
                        jsonObject.put("applicationId", applicationId);
                        System.out.println("一级队列：" + queueName + ",运行的任务id：" + applicationId);
                        String name = app.getName();
                        jsonObject.put("appName", name);
                        // 应用程序的优先级
                        int priority = app.getPriority().getPriority();
                        jsonObject.put("appPriority", priority);
                        YarnApplicationState yarnApplicationState = app.getYarnApplicationState();
                        jsonObject.put("yarnApplicationState", yarnApplicationState.toString());

                        FinalApplicationStatus finalApplicationStatus = app.getFinalApplicationStatus();
                        jsonObject.put("finalApplicationStatus", finalApplicationStatus.toString());
                        String user = app.getUser();
                        jsonObject.put("appUser", user);

                        String applicationType = app.getApplicationType();
                        jsonObject.put("applicationType", applicationType);
                        ApplicationResourceUsageReport applicationResource = app.getApplicationResourceUsageReport();
                        long appUsedMemorySize = applicationResource.getUsedResources().getMemorySize();
                        jsonObject.put("appUsedMemorySize", appUsedMemorySize);
                        // 获取该任务的资源虚拟cpu核心数
                        int virtualCores = applicationResource.getUsedResources().getVirtualCores();
                        jsonObject.put("appVirtualCores", virtualCores);

                        List<ResourceInformation> appUsedResourceInfoList = applicationResource.getUsedResources().getAllResourcesListCopy();
                        jsonObject.put("appUsedResourceInfoList", appUsedResourceInfoList);

                        long appReservedMemorySize = applicationResource.getReservedResources().getMemorySize();
                        jsonObject.put("appReservedMemorySize", appReservedMemorySize);
                        int appReservedVirtualCores = applicationResource.getReservedResources().getVirtualCores();
                        jsonObject.put("appReservedVirtualCores", appReservedVirtualCores);
                        List<ResourceInformation> appReversedResourceInfoList = applicationResource.getReservedResources().getAllResourcesListCopy();
                        jsonObject.put("appReversedResourceInfoList", appReversedResourceInfoList);

                        int appNumUsedContainers = applicationResource.getNumUsedContainers();
                        jsonObject.put("appNumUsedContainers", appNumUsedContainers);

                        int appNumReservedContainers = applicationResource.getNumReservedContainers();
                        jsonObject.put("appNumReservedContainers", appNumReservedContainers);

                        // 获取应用程序使用的集群资源百分比
                        float clusterUsagePercentage = applicationResource.getClusterUsagePercentage();
                        jsonObject.put("clusterUsagePercentage", clusterUsagePercentage);

                        // 获取应用程序正在使用的队列资源百分比
                        float queueUsagePercentage = applicationResource.getQueueUsagePercentage();
                        jsonObject.put("queueUsagePercentage", queueUsagePercentage);

                        long appStartTime = app.getStartTime();
                        jsonObject.put("appStartTime", appStartTime);

                        long appFinishTime = app.getFinishTime();
                        jsonObject.put("appFinishTime", appFinishTime);
                        String appTrackingUrl = app.getTrackingUrl();
                        jsonObject.put("appTrackingUrl", appTrackingUrl);

                        int appRpcPort = app.getRpcPort();
                        jsonObject.put("appRpcPort", appRpcPort);

                        float appProgress = app.getProgress();
                        jsonObject.put("appProgress", appProgress);

                        String appHost = app.getHost();
                        jsonObject.put("appHost", appHost);

                        // 获取所有应用程序容器的默认节点标签表达式
                        String appAmNodeLabelExpression = app.getAmNodeLabelExpression();
                        jsonObject.put("appAmNodeLabelExpression", appAmNodeLabelExpression);

                        Set<String> applicationTags = app.getApplicationTags();
                        jsonObject.put("applicationTags", applicationTags);

                        LogAggregationStatus appLogAggregationStatus = app.getLogAggregationStatus();
                        jsonObject.put("appLogAggregationStatus", appLogAggregationStatus);

                        appsList.add(jsonObject);
                    }
                }
                result.put("appsList", appsList);
                // 一级队列的子队列
                List<QueueInfo> childQueues = item.getChildQueues();
                ArrayList<JSONObject> childQueueList = new ArrayList<>();
                // 二级子队列信息数据获取
                if (childQueues != null) {
                    for (QueueInfo child : childQueues) {
                        JSONObject json = new JSONObject();
                        // 二级子队列的名字
                        String childQueueName = child.getQueueName();
                        json.put("childQueueName", childQueueName);
                        // 二级子队列的每节点标签队列配置
                        Map<String, QueueConfigurations> childQueueConfigurations = child.getQueueConfigurations();
                        json.put("childQueueConfigurations", childQueueConfigurations);
                        // 二级子队列当前状态
                        String childQueueState = child.getQueueState().toString();
                        json.put("childQueueState", childQueueState);
                        QueueStatistics childQueueStatistics = child.getQueueStatistics();
                        json.put("childQueueStatistics", childQueueStatistics);
                        // 二级子队列分配的Container容器个数
                        long childQueueAllocatedContainers = childQueueStatistics.getAllocatedContainers();
                        json.put("childQueueAllocatedContainers", childQueueAllocatedContainers);
                        // 二级子队列被分配的内存大小(以MB为单位)
                        long childQueueAllocatedMemoryMB = childQueueStatistics.getAllocatedMemoryMB();
                        json.put("childQueueAllocatedMemoryMB", childQueueAllocatedMemoryMB);
                        // 二级子队列被分配的vcores
                        long childQueueAllocatedVCores = childQueueStatistics.getAllocatedVCores();
                        json.put("childQueueAllocatedVCores", childQueueAllocatedVCores);
                        // 二级子队列的可用内存(以MB为单位)
                        long childQueueAvailableMemoryMB = childQueueStatistics.getAvailableMemoryMB();
                        json.put("childQueueAvailableMemoryMB", childQueueAvailableMemoryMB);
                        // 二级子队列的可用vcores
                        long childQueueAvailableVCores = childQueueStatistics.getAvailableVCores();
                        json.put("childQueueAvailableVCores", childQueueAvailableVCores);
                        // 二级子队列的活跃用户数
                        long childQueueNumActiveUsers = childQueueStatistics.getNumActiveUsers();
                        json.put("childQueueNumActiveUsers", childQueueNumActiveUsers);
                        // 二级子队列已完成的app个数
                        long childQueueNumAppsCompleted = childQueueStatistics.getNumAppsCompleted();
                        json.put("childQueueNumAppsCompleted", childQueueNumAppsCompleted);
                        // 二级子队列失败的app个数
                        long childQueueNumAppsFailed = childQueueStatistics.getNumAppsFailed();
                        json.put("childQueueNumAppsFailed", childQueueNumAppsFailed);
                        // 二级子队列被kill掉的app个数
                        long childQueueNumAppsKilled = childQueueStatistics.getNumAppsKilled();
                        json.put("childQueueNumAppsKilled", childQueueNumAppsKilled);
                        // 二级子队列状态为pending的app个数
                        long childQueueNumAppsPending = childQueueStatistics.getNumAppsPending();
                        json.put("childQueueNumAppsPending", childQueueNumAppsPending);
                        // 二级子队列正在运行的app个数
                        long childQueueNumAppsRunning = childQueueStatistics.getNumAppsRunning();
                        json.put("childQueueNumAppsRunning", childQueueNumAppsRunning);
                        // 二级子队列提交的app数量
                        long childQueueNumAppsSubmitted = childQueueStatistics.getNumAppsSubmitted();
                        json.put("childQueueNumAppsSubmitted", childQueueNumAppsSubmitted);
                        // 二级子队列待处理container容器的数量
                        long childQueuePendingContainers = childQueueStatistics.getPendingContainers();
                        json.put("childQueuePendingContainers", childQueuePendingContainers);
                        // 二级子队列待处理的内存(MB)
                        long childQueuePendingMemoryMB = childQueueStatistics.getPendingMemoryMB();
                        json.put("childQueuePendingMemoryMB", childQueuePendingMemoryMB);
                        // 二级子队列待处理的vcores
                        long childQueuePendingVCores = childQueueStatistics.getPendingVCores();
                        json.put("childQueuePendingVCores", childQueuePendingVCores);
                        // 二级子队列预留Container容器的数量
                        long childQueueReservedContainers = childQueueStatistics.getReservedContainers();
                        json.put("childQueueReservedContainers", childQueueReservedContainers);
                        // 二级子队列预留的内存
                        long childQueueReservedMemoryMB = childQueueStatistics.getReservedMemoryMB();
                        json.put("childQueueReservedMemoryMB", childQueueReservedMemoryMB);
                        // 二级子队列预留的vcores
                        long childQueueReservedVCores = childQueueStatistics.getReservedVCores();
                        json.put("childQueueReservedVCores", childQueueReservedVCores);

                        // 二级子队列的可访问节点标签
                        Set<String> childQueueAccessibleNodeLabels = child.getAccessibleNodeLabels();
                        json.put("childQueueAccessibleNodeLabels", childQueueAccessibleNodeLabels);
                        // 二级子队列的配置容量
                        float childQueueCapacity = child.getCapacity();
                        json.put("childQueueCapacity", childQueueCapacity);
                        // 二级子队列的当前容量
                        float childQueueCurrentCapacity = child.getCurrentCapacity();
                        json.put("childQueueCurrentCapacity", childQueueCurrentCapacity);
                        // 二级子队列的默认节点标签表达式
                        String childQueueDefaultNodeLabelExpression = child.getDefaultNodeLabelExpression();
                        json.put("childQueueDefaultNodeLabelExpression", childQueueDefaultNodeLabelExpression);
                        // 二级子队列的队列内抢占状态, 如果属性不在proto中，则返回null； 否则返回队列的队列内抢占状态
                        Boolean childQueueIntraQueuePreemptionDisabled = child.getIntraQueuePreemptionDisabled();
                        json.put("childQueueIntraQueuePreemptionDisabled", childQueueIntraQueuePreemptionDisabled);
                        // 二级子队列的最大容量
                        float childQueueMaximumCapacity = child.getMaximumCapacity();
                        json.put("childQueueMaximumCapacity", childQueueMaximumCapacity);
                        // 二级子队列的抢占状态.如果属性不在proto中，则返回null； 否则返回队列的抢占状态
                        Boolean childQueuePreemptionDisabled = child.getPreemptionDisabled();
                        json.put("childQueuePreemptionDisabled", childQueuePreemptionDisabled);
                        // 获取子队列正在运行的任务信息
                        List<ApplicationReport> apps = child.getApplications();
                        ArrayList<JSONObject> childAppsList = new ArrayList<>();
                        if (apps != null) {
                            for (ApplicationReport app : apps) {
                                JSONObject tempJson = new JSONObject();
                                ApplicationId applicationId = app.getApplicationId();
                                tempJson.put("applicationId", applicationId);
                                String name = app.getName();
                                tempJson.put("appName", name);
                                // 应用程序的优先级
                                int priority = app.getPriority().getPriority();
                                tempJson.put("appPriority", priority);
                                YarnApplicationState yarnApplicationState = app.getYarnApplicationState();
                                tempJson.put("yarnApplicationState", yarnApplicationState.toString());

                                FinalApplicationStatus finalApplicationStatus = app.getFinalApplicationStatus();
                                tempJson.put("finalApplicationStatus", finalApplicationStatus.toString());
                                String user = app.getUser();
                                tempJson.put("appUser", user);

                                String applicationType = app.getApplicationType();
                                tempJson.put("applicationType", applicationType);
                                ApplicationResourceUsageReport applicationResource = app.getApplicationResourceUsageReport();
                                long appUsedMemorySize = applicationResource.getUsedResources().getMemorySize();
                                tempJson.put("appUsedMemorySize", appUsedMemorySize);
                                // 获取该任务的资源虚拟cpu核心数
                                int virtualCores = applicationResource.getUsedResources().getVirtualCores();
                                tempJson.put("appVirtualCores", virtualCores);

                                List<ResourceInformation> appUsedResourceInfoList = applicationResource.getUsedResources().getAllResourcesListCopy();
                                tempJson.put("appUsedResourceInfoList", appUsedResourceInfoList);

                                long appReservedMemorySize = applicationResource.getReservedResources().getMemorySize();
                                tempJson.put("appReservedMemorySize", appReservedMemorySize);
                                int appReservedVirtualCores = applicationResource.getReservedResources().getVirtualCores();
                                tempJson.put("appReservedVirtualCores", appReservedVirtualCores);
                                List<ResourceInformation> appReversedResourceInfoList = applicationResource.getReservedResources().getAllResourcesListCopy();
                                tempJson.put("appReversedResourceInfoList", appReversedResourceInfoList);

                                int appNumUsedContainers = applicationResource.getNumUsedContainers();
                                tempJson.put("appNumUsedContainers", appNumUsedContainers);

                                int appNumReservedContainers = applicationResource.getNumReservedContainers();
                                tempJson.put("appNumReservedContainers", appNumReservedContainers);

                                // 获取应用程序使用的集群资源百分比
                                float clusterUsagePercentage = applicationResource.getClusterUsagePercentage();
                                tempJson.put("clusterUsagePercentage", clusterUsagePercentage);

                                // 获取应用程序正在使用的队列资源百分比
                                float queueUsagePercentage = applicationResource.getQueueUsagePercentage();
                                tempJson.put("queueUsagePercentage", queueUsagePercentage);

                                long appStartTime = app.getStartTime();
                                tempJson.put("appStartTime", appStartTime);

                                long appFinishTime = app.getFinishTime();
                                tempJson.put("appFinishTime", appFinishTime);
                                String appTrackingUrl = app.getTrackingUrl();
                                tempJson.put("appTrackingUrl", appTrackingUrl);

                                int appRpcPort = app.getRpcPort();
                                tempJson.put("appRpcPort", appRpcPort);

                                float appProgress = app.getProgress();
                                tempJson.put("appProgress", appProgress);

                                String appHost = app.getHost();
                                tempJson.put("appHost", appHost);

                                // 获取所有应用程序容器的默认节点标签表达式
                                String appAmNodeLabelExpression = app.getAmNodeLabelExpression();
                                tempJson.put("appAmNodeLabelExpression", appAmNodeLabelExpression);

                                Set<String> applicationTags = app.getApplicationTags();
                                tempJson.put("applicationTags", applicationTags);

                                LogAggregationStatus appLogAggregationStatus = app.getLogAggregationStatus();
                                tempJson.put("appLogAggregationStatus", appLogAggregationStatus);

                                childAppsList.add(tempJson);
                            }
                            json.put("childAppsList", childAppsList);
                        }
                        childQueueList.add(json);
                    }
                }
                result.put("childQueueList", childQueueList);
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
