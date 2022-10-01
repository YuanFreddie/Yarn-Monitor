package com.project.bigdata.apps;

import com.project.bigdata.entity.YarnApplicationInfo;
import com.project.bigdata.source.FlinkMetricsSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkMetricMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        DataStreamSource<YarnApplicationInfo> flinkAppSource = env.addSource(new FlinkMetricsSource());

        env.execute("FlinkMetricMonitor");
    }
}
