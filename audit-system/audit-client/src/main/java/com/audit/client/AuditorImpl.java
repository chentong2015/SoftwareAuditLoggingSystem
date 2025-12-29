package com.audit.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import static com.audit.client.AuditQueueManager.MAX_LENGTH_DATA_RAW;
import static com.audit.client.AuditQueueManager.MAX_QUEUE_SIZE;

// Auditor包含整个程序的核心逻辑
public class AuditorImpl {

    private final AuditQueueManager auditQueueManager;

    // 创建发送Audit的异步线程并启动
    public AuditorImpl()  {
        auditQueueManager = new AuditQueueManager();
        Thread auditTaskSenderThread = new Thread(new AsyncAuditTaskSender());
        auditTaskSenderThread.start();
    }

    // TODO. 将Audit压入到Queue队列中等待异步线程处理
    public void log(AuditEntry entry) {
        if (entry != null) {
            auditQueueManager.queueNewAudit(entry);
        }
    }

    // TODO. 异步线程, 周期性的循环发送Audit事件
    class AsyncAuditTaskSender implements Runnable {

        @Override
        public void run() {
            auditQueueManager.restoreMissingAudits();
            while (true) {
                try {
                    sendWaitingEvents();
                    Thread.sleep(5000);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        // TODO. 发送HTTP请求到Audit Service后端服务
        private void sendWaitingEvents() throws IOException {
            if (auditQueueManager.isQueueEmpty()) {
                return;
            }

            long nbAudits = auditQueueManager.sendQueueAudits(entry -> {
                if (!isValidDataRawSize(entry)) {
                    return true;
                }
                return sendHttpRequest();
            });

            if (nbAudits > MAX_QUEUE_SIZE) {
                System.out.println("Event queue size is full");
            }
        }

        // Log error and ignore sending large payload data raw event
        private boolean isValidDataRawSize(AuditEntry auditEntry) {
            ObjectMapper jsonMapper = new ObjectMapper();
            try {
                String strDataRaw = jsonMapper.writeValueAsString(auditEntry.getObjectProperties());
                if (!strDataRaw.isEmpty() && strDataRaw.getBytes(StandardCharsets.UTF_8).length > MAX_LENGTH_DATA_RAW) {
                    System.out.println("Payload size has more than " +  MAX_LENGTH_DATA_RAW);
                    return false;
                }
                return true;
            } catch (Exception exception) {
                exception.printStackTrace();
                return false;
            }
        }

        // 使用随机值默认日志的发送异常
        private boolean sendHttpRequest() {
            Random random = new Random();
            String url = "http://localhost:8080/v1/log/";
            url = url + random.nextInt(4, 10);

            RestTemplate restTemplate = new RestTemplate();
            String response = restTemplate.getForObject(url, String.class);
            return response != null && response.equals("success");
        }
    }
}
