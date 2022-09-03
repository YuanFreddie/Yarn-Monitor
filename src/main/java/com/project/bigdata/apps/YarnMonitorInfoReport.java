package com.project.bigdata.apps;

import com.alibaba.fastjson.JSONObject;
import com.project.bigdata.source.YarnNodeInfoSource;
import com.project.bigdata.source.YarnQueueInfoSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class YarnMonitorInfoReport {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // source
        DataStreamSource<JSONObject> queueJson = env.addSource(new YarnQueueInfoSource());
        DataStreamSource<JSONObject> nodeSource = env.addSource(new YarnNodeInfoSource());

        // transformation

        // sink
        queueJson.print("queueData===>");
        nodeSource.print("nodeData===>");

        env.execute("yarn_monitor_info_report");
    }
}
