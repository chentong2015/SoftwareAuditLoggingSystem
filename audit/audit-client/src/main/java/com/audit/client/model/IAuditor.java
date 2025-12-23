package com.audit.client.model;

public interface IAuditor {

    void log(AuditEntry entry);
}
