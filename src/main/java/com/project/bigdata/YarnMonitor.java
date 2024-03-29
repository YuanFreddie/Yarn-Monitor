package com.project.bigdata;

import com.alibaba.fastjson.JSONObject;
import com.project.bigdata.source.ApplicationSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class YarnMonitor {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        // 读取yarn任务数据源
        DataStreamSource<JSONObject> sourceStream = env.addSource(new ApplicationSource());

        // 计算已经结束的任务的资源消耗
        SingleOutputStreamOperator<String> sinkStream = sourceStream.map((MapFunction<JSONObject, String>) input -> {
            Integer usedMemory = input.getInteger("usedMemory");
            Integer usedCores = input.getInteger("usedCores");
            if (usedMemory == -1 && usedCores == -1) {
                Long finishTime = input.getLong("finishTime");
                Long launchTime = input.getLong("launchTime");
                Long memorySeconds = input.getLong("memorySeconds");
                Long vCoreSeconds = input.getLong("vcoreSeconds");
                input.put("usedMemory", memorySeconds * 1000 / (finishTime - launchTime));
                input.put("usedCores", vCoreSeconds * 1000 / (finishTime - launchTime));
            }
            return input.toString();
        }).returns(String.class);

        // 写数据到kafka
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "master01:9092,master02:9092");
        FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>("yarn_application_monitor", new SimpleStringSchema(), prop);
        sinkStream.addSink(producer);
        env.execute();
    }
}
