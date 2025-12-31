package org.example;

import com.audit.client.AuditEntry;
import com.audit.client.AuditQueueManager;
import com.audit.client.util.HttpHelper;

import java.io.IOException;

public class AuditQueueManagerApplication {

    public static void main(String[] args) throws IOException {
        AuditQueueManager auditQueueManager = new AuditQueueManager();

        // 模拟新Audit事件的入队操作
        AuditEntry entry1 = new AuditEntry();
        entry1.setId("0001");
        entry1.setOperatorId("100");
        auditQueueManager.queueNewAudit(entry1);

        // 模拟AuditImpl异步线程发送的流程
        auditQueueManager.restoreMissingAudits();
        long nbAudits = auditQueueManager.sendQueueAudits(entry -> HttpHelper.sendGetRequest());
        System.out.println(nbAudits);
    }
}
