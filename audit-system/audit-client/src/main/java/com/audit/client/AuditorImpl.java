package com.audit.client;

import com.audit.client.util.HttpHelper;

import java.io.*;

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

            // 返回true才能将audit清除，避免线程的无限循环
            long nbAudits = auditQueueManager.sendQueueAudits(entry -> {
                if (entry.isValidDataRawSize()) {
                    return true;
                }
                return HttpHelper.sendGetRequest();
            });
            if (nbAudits > MAX_QUEUE_SIZE) {
                System.out.println("Event queue size is full");
            }
        }
    }
}
