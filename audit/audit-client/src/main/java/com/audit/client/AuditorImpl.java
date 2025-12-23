package com.audit.client;

import com.audit.client.model.AuditEntry;
import com.audit.client.model.IAuditor;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Path;
import java.util.function.Function;

import static com.audit.client.AuditQueueManager.MAX_QUEUE_SIZE;

public class AuditorImpl implements IAuditor {

    private final ObjectMapper mapper;
    private final AuditQueueManager auditQueueManager;
    private final Thread auditTaskSenderThread;

    public AuditorImpl() throws IOException {
        mapper = new ObjectMapper();
        auditQueueManager = new AuditQueueManager();

        auditTaskSenderThread = new Thread(new AsyncAuditTaskSender());
        auditTaskSenderThread.start();
    }

    // TODO. 将Audit Event压入到Queue队列中, 等待异步线程的处理
    @Override
    public void log(AuditEntry entry) {
        if (null == entry) {
            return;
        }
        auditQueueManager.queueNewAudit(entry);
    }

    // 创建".tmp"临时文件来发送Queue JSON文件中所有事件
    public long sendQueueAudits(Function<AuditEntry, Boolean> sender) throws IOException {
        long auditCount = 0L;
        Path toSendPath = null;
        try {
            toSendPath = auditQueueManager.getPendingAudits();
            if (null == toSendPath) {
                return auditCount;
            }
            File toSendFile = toSendPath.toFile();
            if (!toSendFile.exists() || toSendFile.length() == 0) {
                return auditCount;
            }

            String line;
            File tmpFile = new File(toSendFile.getAbsolutePath() + ".tmp");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new FileInputStream(toSendFile), "UTF-8"))) {
                while ((line = reader.readLine()) != null) {
                    try {
                        ++auditCount;
                        sendSingleAudit(line, tmpFile, sender);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            auditQueueManager.requeueMissingAuditsAndDelete(toSendPath);
        }
        return auditCount;
    }

    private void sendSingleAudit(String line, File tmpFile, Function<AuditEntry, Boolean> sender) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
                new FileOutputStream(tmpFile, false), "UTF-8"))) {
            writer.write(line);
            writer.newLine();
        }

        AuditEntry auditEntry = mapper.readValue(line, AuditEntry.class);
        boolean status = sender.apply(auditEntry);
        if (!status) {
            tmpFile.delete();
        } else {
           // auditQueueManager.completeSendAudit()
        }
    }

    class AsyncAuditTaskSender implements Runnable {

        // TODO. 周期性的循环发送Audit事件
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

        // TODO. 发送HTTP请求到Audit-Service存储日志数据
        private void sendWaitingEvents() throws IOException {
            if (auditQueueManager.isQueueEmpty()) {
                return;
            }

            long nbAudits = sendQueueAudits(entry -> {
                // RestRequest request = new RestRequest("/v1/log", entry);
                // RestResponse<?> response = postClientForSendingEvents.query(request);
                // return response.isSuccess();
                return true;
            });

            if (nbAudits > MAX_QUEUE_SIZE) {
                System.out.println("Event queue size is full");
            }
        }
    }
}
