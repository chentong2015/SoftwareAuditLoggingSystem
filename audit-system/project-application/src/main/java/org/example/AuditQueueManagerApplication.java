package org.example;

import com.audit.client.AuditEntry;
import com.audit.client.AuditQueueManager;
import com.audit.client.util.HttpHelper;

import java.io.IOException;

public class AuditQueueManagerApplication {

    public static void main(String[] args) throws IOException {
        AuditQueueManager auditQueueManager = new AuditQueueManager();

        // 测试写入新的Audit Event事件
        AuditEntry entry1 = new AuditEntry();
        entry1.setId("0001");
        entry1.setOperatorId("100");
        auditQueueManager.queueNewAudit(entry1);

        // 测试Audit的完整发送流程
        auditQueueManager.restoreMissingAudits();
        long nbAudits = auditQueueManager.sendQueueAudits(entry -> {
            if (entry.isValidDataRawSize()) {
                return true;
            }
            return HttpHelper.sendGetRequest();
        });
        System.out.println(nbAudits);
    }
}
