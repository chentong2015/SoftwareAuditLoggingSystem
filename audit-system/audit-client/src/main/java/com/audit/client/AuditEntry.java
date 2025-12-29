package com.audit.client;

import java.util.Date;
import java.util.Map;

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
        return objectProperties;
    }

    public void setObjectProperties(Map<String, String> objectProperties) {
        this.objectProperties = objectProperties;
    }
}
