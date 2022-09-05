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
                // 队列当前状态
                String queueState = item.getQueueState().toString();
                result.put("queueState", queueState);
                QueueStatistics queueStatistics = item.getQueueStatistics();
                if (queueStatistics != null) {
                    // 队列分配的Container容器个数
                    long allocatedContainers = queueStatistics.getAllocatedContainers();
                    result.put("allocatedContainers", allocatedContainers);
                    // 队列被分配的内存大小(以MB为单位)
                    long allocatedMemoryMB = queueStatistics.getAllocatedMemoryMB();
                    result.put("allocatedMemoryMB", allocatedMemoryMB);
                    // 队列被分配的vcores
                    long allocatedVCores = queueStatistics.getAllocatedVCores();
                    result.put("allocatedVCores", allocatedVCores);
                    // 队列的可用内存(以MB为单位)
                    long availableMemoryMB = queueStatistics.getAvailableMemoryMB();
                    result.put("availableMemoryMB", availableMemoryMB);
                    // 队列的可用vcores
                    long availableVCores = queueStatistics.getAvailableVCores();
                    result.put("availableVCores", availableVCores);
                    // 队列的活跃用户数
                    long numActiveUsers = queueStatistics.getNumActiveUsers();
                    result.put("numActiveUsers", numActiveUsers);
                    // 队列已完成的app个数
                    long numAppsCompleted = queueStatistics.getNumAppsCompleted();
                    result.put("numAppsCompleted", numAppsCompleted);
                    // 队列失败的app个数
                    long numAppsFailed = queueStatistics.getNumAppsFailed();
                    result.put("numAppsFailed", numAppsFailed);
                    // 队列被kill掉的app个数
                    long numAppsKilled = queueStatistics.getNumAppsKilled();
                    result.put("numAppsKilled", numAppsKilled);
                    // 队列状态为pending的app个数
                    long numAppsPending = queueStatistics.getNumAppsPending();
                    result.put("numAppsPending", numAppsPending);
                    // 队列正在运行的app个数
                    long numAppsRunning = queueStatistics.getNumAppsRunning();
                    result.put("numAppsRunning", numAppsRunning);
                    // 队列提交的app数量
                    long numAppsSubmitted = queueStatistics.getNumAppsSubmitted();
                    result.put("numAppsSubmitted", numAppsSubmitted);
                    // 队列待处理container容器的数量
                    long pendingContainers = queueStatistics.getPendingContainers();
                    result.put("pendingContainers", pendingContainers);
                    // 队列待处理的内存(MB)
                    long pendingMemoryMB = queueStatistics.getPendingMemoryMB();
                    result.put("pendingMemoryMB", pendingMemoryMB);
                    // 队列待处理的vcores
                    long pendingVCores = queueStatistics.getPendingVCores();
                    result.put("pendingVCores", pendingVCores);
                    // 队列预留Container容器的数量
                    long reservedContainers = queueStatistics.getReservedContainers();
                    result.put("reservedContainers", reservedContainers);
                    // 队列预留的内存
                    long reservedMemoryMB = queueStatistics.getReservedMemoryMB();
                    result.put("reservedMemoryMB", reservedMemoryMB);
                    // 队列预留的vcores
                    long reservedVCores = queueStatistics.getReservedVCores();
                    result.put("reservedVCores", reservedVCores);
                }

                // 队列的可访问节点标签
                Set<String> accessibleNodeLabels = item.getAccessibleNodeLabels();
                result.put("accessibleNodeLabels", accessibleNodeLabels);
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
                List<QueueInfo> childQueues = item.getChildQueues();
                ArrayList<JSONObject> childQueueInfo = new ArrayList<>();
                for (QueueInfo child : childQueues) {
                    JSONObject tempJson = new JSONObject();
                    String childQueueName = child.getQueueName();
                    tempJson.put("childQueueName", childQueueName);

                    childQueueInfo.add(tempJson);
                }

                result.put("childQueueInfoList", childQueueInfo);
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
