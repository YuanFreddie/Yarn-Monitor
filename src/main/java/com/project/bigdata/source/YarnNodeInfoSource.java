package com.project.bigdata.source;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class YarnNodeInfoSource extends RichSourceFunction<JSONObject> {
    YarnClient yarnClient;

    @Override
    public void open(Configuration parameters) {
        yarnClient = YarnClient.createYarnClient();
        org.apache.hadoop.conf.Configuration conf = new org.apache.hadoop.conf.Configuration();
        yarnClient.init(conf);
        yarnClient.start();
    }

    @Override
    public void run(SourceContext<JSONObject> collect) throws Exception {
        while (true) {
            List<NodeReport> nodeReports = yarnClient.getNodeReports();
            JSONObject resultJson = new JSONObject();
            YarnClusterMetrics yarnClusterMetrics = yarnClient.getYarnClusterMetrics();
            // 获取集群nodeManager的总数
            int numNodeManagers = yarnClusterMetrics.getNumNodeManagers();
            resultJson.put("numNodeManagers", numNodeManagers);
            // 获取集群中活着的NodeManager数量
            int numActiveNodeManagers = yarnClusterMetrics.getNumActiveNodeManagers();
            resultJson.put("numActiveNodeManagers", numActiveNodeManagers);
            // 获取集群中退役的NodeManager数量
            int numDecommissionedNodeManagers = yarnClusterMetrics.getNumDecommissionedNodeManagers();
            resultJson.put("numDecommissionedNodeManagers", numDecommissionedNodeManagers);
            // 获取集群中失去心跳的nodeManager数量
            int numLostNodeManagers = yarnClusterMetrics.getNumLostNodeManagers();
            resultJson.put("numLostNodeManagers", numLostNodeManagers);
            // 获取集群中不健康的NodeManager数量
            int numUnhealthyNodeManagers = yarnClusterMetrics.getNumUnhealthyNodeManagers();
            resultJson.put("numUnhealthyNodeManagers", numUnhealthyNodeManagers);
            // 获取集群中重新启动的NodeManager数量
            int numRebootedNodeManagers = yarnClusterMetrics.getNumRebootedNodeManagers();
            resultJson.put("numRebootedNodeManagers", numRebootedNodeManagers);
            for (NodeReport node : nodeReports) {
                JSONObject jsonObject = new JSONObject();
                NodeId nodeId = node.getNodeId();
                // 该节点的host
                String host = nodeId.getHost();
                jsonObject.put("host", host);
                // 该节点的port
                int port = nodeId.getPort();
                jsonObject.put("port", port);
                // 获取节点的总资源信息
                Resource capability = node.getCapability();
                List<ResourceInformation> list = capability.getAllResourcesListCopy();
                jsonObject.put("totalResourceInformationList", list);
                // 获取该节点的总虚拟CPU核心数
                int virtualCores = capability.getVirtualCores();
                jsonObject.put("virtualCores", virtualCores);
                // 获取该节点资源的总内存,以MB为单位的内存
                long memorySize = capability.getMemorySize();
                jsonObject.put("memorySize", memorySize);
                // 获取节点状态
                NodeState nodeState = node.getNodeState();
                jsonObject.put("nodeState", nodeState.toString());
                // 获取节点的http地址
                String httpAddress = node.getHttpAddress();
                jsonObject.put("httpAddress", httpAddress);
                // 获取该节点的标签
                Set<String> nodeLabels = node.getNodeLabels();
                jsonObject.put("nodeLabels", nodeLabels);
                // 获取该节点资源利用情况数据
                ResourceUtilization nodeResource = node.getAggregatedContainersUtilization();
                // 获取该节点使用的虚拟内存
                int virtualMemory = nodeResource.getVirtualMemory();
                jsonObject.put("virtualMemory", virtualMemory);
                // 获取cpu的利用率
                float cpu = nodeResource.getCPU();
                jsonObject.put("cpu", cpu);
                // 获取该节点的物理内存，MB
                int physicalMemory = nodeResource.getPhysicalMemory();
                jsonObject.put("physicalMemory", physicalMemory);
                // 获取节点的诊断健康报告
                String healthReport = node.getHealthReport();
                jsonObject.put("healthReport", healthReport);
                // 获取节点收到健康报告的最后时间戳
                long lastHealthReportTime = node.getLastHealthReportTime();
                jsonObject.put("lastHealthReportTime", lastHealthReportTime);
                // 获取节点更新类型，null表示不存在更新类型
                NodeUpdateType nodeUpdateType = node.getNodeUpdateType();
                jsonObject.put("nodeUpdateType", nodeUpdateType);
                // 获取节点上分配的容器数
                int numContainers = node.getNumContainers();
                jsonObject.put("numContainers", numContainers);
                // 获取节点的机架名称
                String rackName = node.getRackName();
                jsonObject.put("rackName", rackName);
                // 获取节点上正在使用的资源信息
                Resource usedResource = node.getUsed();
                if (usedResource != null) {
                    // 获取该节点正在被使用的内存
                    long usedMemorySize = usedResource.getMemorySize();
                    jsonObject.put("usedMemorySize", usedMemorySize);
                    // 获取该节点正在被使用的vcores
                    int usedVirtualCores = usedResource.getVirtualCores();
                    jsonObject.put("usedVirtualCores", usedVirtualCores);
                    // 获取正在被使用的资源信息列表
                    List<ResourceInformation> allResourcesListCopy = usedResource.getAllResourcesListCopy();
                    jsonObject.put("usedResourceInformationList", allResourcesListCopy);
                }
                resultJson.put("nodeManagerInfo", jsonObject);
            }
            collect.collect(resultJson);
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
