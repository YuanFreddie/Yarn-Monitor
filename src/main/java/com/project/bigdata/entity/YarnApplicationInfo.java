package com.project.bigdata.entity;

import lombok.Data;

@Data
public class YarnApplicationInfo {
    public String applicationId;

    public String applicationType;

    public String userName;

    public String appName;

    public String queue;

    public Long usedMemory;

    public Integer usedVCores;

    public Long startTime;

    public Long finishTime;

    public String applicationState;

    public String trackingUrl;

    public YarnApplicationInfo(String applicationId, String applicationType, String userName, String appName, String queue, Long usedMemory, Integer usedVCores, Long startTime, Long finishTime, String applicationState, String trackingUrl) {
        this.applicationId = applicationId;
        this.applicationType = applicationType;
        this.userName = userName;
        this.appName = appName;
        this.queue = queue;
        this.usedMemory = usedMemory;
        this.usedVCores = usedVCores;
        this.startTime = startTime;
        this.finishTime = finishTime;
        this.applicationState = applicationState;
        this.trackingUrl = trackingUrl;
    }
}
