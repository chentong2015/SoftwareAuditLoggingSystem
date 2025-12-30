package org.example;

import com.audit.client.AuditEntry;
import com.audit.client.AuditorImpl;

import java.util.HashMap;

public class AuditorImplApplication {

    // 测试发送Audit Event后自动异步线程处理
    public static void main(String[] args) throws InterruptedException {
        AuditorImpl auditorImpl = new AuditorImpl();

        AuditEntry entry1 = new AuditEntry();
        entry1.setId("0001");
        entry1.setOperatorId("100");
        auditorImpl.log(entry1);

        Thread.sleep(5000);

        AuditEntry entry2 = new AuditEntry();
        entry2.setId("0002");
        entry2.setOperatorId("200");
        auditorImpl.log(entry2);

        Thread.sleep(5000);

        // 模拟Payload负载过大的异常
        HashMap<String, String> map = new HashMap<>();
        map.put("key1", "value 1");
        map.put("key2", "value 2");
        AuditEntry entry3 = new AuditEntry();
        entry3.setId("0003");
        entry3.setOperatorId("300");
        entry3.setObjectProperties(map);
        auditorImpl.log(entry3);
    }
}
