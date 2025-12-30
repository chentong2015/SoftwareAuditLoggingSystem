package com.audit.client;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.audit.client.AuditQueueManager.MAX_LENGTH_DATA_RAW;

public class AuditEntry {

    private String id;
    private String eventId;
    private Date timestamp;
    private String operatorId;
    private String operatorName;
    private String comments;
    private Map<String, String> objectProperties;

    public AuditEntry(){
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public String getOperatorId() {
        return operatorId;
    }

    public void setOperatorId(String operatorId) {
        this.operatorId = operatorId;
    }

    public String getOperatorName() {
        return operatorName;
    }

    public void setOperatorName(String operatorName) {
        this.operatorName = operatorName;
    }

    public String getComments() {
        return comments;
    }

    public void setComments(String comments) {
        this.comments = comments;
    }

    public Map<String, String> getObjectProperties() {
        if (this.objectProperties == null) {
            this.setObjectProperties(new HashMap<>());
        }
        return objectProperties;
    }

    public void setObjectProperties(Map<String, String> objectProperties) {
        this.objectProperties = objectProperties;
    }

    // TODO. 判断当前Audit对象是否有效
    // Log error and ignore sending large payload data raw event
    public boolean isValidDataRawSize() {
        ObjectMapper jsonMapper = new ObjectMapper();
        try {
            String strDataRaw = jsonMapper.writeValueAsString(getObjectProperties());
            if (!strDataRaw.isEmpty() && strDataRaw.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH_DATA_RAW) {
                System.out.println("Payload size has more than " +  MAX_LENGTH_DATA_RAW);
                return true;
            }
            return false;
        } catch (Exception exception) {
            exception.printStackTrace();
            return true;
        }
    }
}
