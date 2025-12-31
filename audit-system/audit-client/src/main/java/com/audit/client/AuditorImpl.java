package com.audit.client;

import com.audit.client.util.HttpHelper;

import java.io.*;

import static com.audit.client.AuditQueueManager.MAX_QUEUE_SIZE;

// Auditor包含核心逻辑: 公开log() API接口
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
                    Thread.sleep(30000);
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
            long nbAudits = auditQueueManager.sendQueueAudits(entry -> HttpHelper.sendGetRequest());
            if (nbAudits > MAX_QUEUE_SIZE) {
                System.out.println("Event queue size is full");
            }
        }
    }
}
