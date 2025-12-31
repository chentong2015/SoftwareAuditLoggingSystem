package com.audit.client;

import com.audit.client.util.FileHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;

// 关于Audit Queue队列管理器: 文件数据的操作和处理
public class AuditQueueManager {

    public final static int MAX_QUEUE_SIZE = 1000;
    public final static int MAX_LENGTH_DATA_RAW = 20;

    // TODO. 取当前项目的根目录相对路径
    private File queueJsonFile;
    private String queueFileName = "./audit-system/audit/eventqueue.json";

    private final static String TO_SEND_FILE_SUFFIX = ".to_send.txt";
    private final static String ACKNOWLEDGE_STRING = ".result.OK.";
    private final static String ACKNOWLEDGE_FILE_PATTERN = ".result.OK.%06d.tmp";
    private final static String REQUEUE_FILE_SUFFIX = ".requeue.tmp";

    private final ObjectMapper jsonMapper;
    private Set<String> auditAlreadySent;

    // 创建用于存储Audits数据的JSON文件
    public AuditQueueManager()  {
        jsonMapper = new ObjectMapper();

        this.queueJsonFile = new File(queueFileName);
        this.queueJsonFile.getParentFile().mkdirs();
        try {
            this.queueJsonFile.createNewFile();
        } catch (Exception exception) {
            throw new RuntimeException(exception.getMessage());
        }
    }

    // 判断是否有Audit Event在队列中(一行代表一个Event)
    public boolean isQueueEmpty() {
        synchronized (queueJsonFile) {
            return (queueJsonFile.length() == 0);
        }
    }

    // 写入到Queue JSON文件时Event必须加锁，保证不会丢失Audit
    public void queueNewAudit(AuditEntry auditEntry) {
        if (!isValidDataRawSize(auditEntry)) {
            return;
        }
        synchronized (queueJsonFile) {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(queueJsonFile, true), "UTF-8"))) {
                writer.write(jsonMapper.writeValueAsString(auditEntry));
                writer.newLine();
            } catch (Exception e) {
                System.out.println("Unable to queue audit event list " + e.getMessage());
            }
        }
    }

    // TODO. 入队列之前判断Audit对象是否有效, 无法发送的Audit没有添加到队列的必要
    // Log error and ignore sending large payload data raw event
    private boolean isValidDataRawSize(AuditEntry auditEntry) {
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

    // 创建".tmp"临时文件来发送Queue JSON文件中所有事件
    public long sendQueueAudits(Function<AuditEntry, Boolean> sender) throws IOException {
        long auditCount = 0L;
        Path toSendPath = null;
        try {
            // 从Queue JOSN文件中取出pending待发送Audits，放到"toSend.xml"文件中
            toSendPath = getPendingAudits();
            if (null == toSendPath) {
                return auditCount;
            }
            File toSendFile = toSendPath.toFile();
            if (!toSendFile.exists() || toSendFile.length() == 0) {
                return auditCount;
            }

            // 创建".tmp"临时路径对象来处理所有待发送的Audit
            File tmpFile = new File(toSendFile.getAbsolutePath() + ".tmp");
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(toSendFile), "UTF-8"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    try {
                        ++auditCount;
                        // TODO. 每一行创建独立的空文件写入，不使用append模式，每个文件存储一行数据
                        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpFile, false), "UTF-8"))) {
                            writer.write(line);
                            writer.newLine();
                        }

                        AuditEntry auditEntry = jsonMapper.readValue(line, AuditEntry.class);
                        boolean status = sender.apply(auditEntry);
                        if (status) {
                            System.out.println("Audit Entry :" + auditEntry.getId() + "send successfully !");
                            completeSendAudit(toSendFile, tmpFile, auditCount);
                        } else {
                            tmpFile.delete();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
            // 保证最后清理发送的Audit, 同时删除中间临时文件
            requeueMissingAuditsAndDelete(toSendPath);
        }
        return auditCount;
    }

    // TODO. 将Queue JSON文件数据拷贝到待处理文件中, 重新创建新JSON文件
    // Return an unique audit queue copy file and reset the main audit queue
    private Path getPendingAudits() {
        synchronized (queueJsonFile) {
            if (!isQueueEmpty() && queueJsonFile.exists()) {
                try {
                    String toSendPath = queueJsonFile.getAbsolutePath() + "_" + UUID.randomUUID() + TO_SEND_FILE_SUFFIX;
                    Path pendingQueue = Paths.get(toSendPath).toAbsolutePath().normalize();
                    Files.move(queueJsonFile.toPath(), pendingQueue, StandardCopyOption.ATOMIC_MOVE);
                    queueJsonFile.createNewFile();
                    return pendingQueue;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            return null;
        }
    }

    // TODO. 完成单一Audit事件的发送，处理tmp临时文件
    // 如果成功则将tmp改成完成文件(名称基于toSend文件名)，以便过滤和发送后续Audit
    private void completeSendAudit(File toSendFile, File tmpFile, long auditCount) throws IOException {
        String finalFileName = String.format(toSendFile.getAbsolutePath() + ACKNOWLEDGE_FILE_PATTERN, auditCount);
        Path finalStatusPath = Paths.get(finalFileName).toAbsolutePath().normalize();
        Files.move(tmpFile.toPath(), finalStatusPath, StandardCopyOption.ATOMIC_MOVE);
    }

    // TODO. 如果出现异常，需要先将"toSend.xml"文件中剩余Audit进行恢复
    // Restore missing audit events from temporary files in case of crash
    public void restoreMissingAudits() {
        FileHelper.applyOnFiles( queueJsonFile, queueJsonFile.getName(),
        path -> {
            if (path.toString().endsWith(TO_SEND_FILE_SUFFIX)) {
               requeueMissingAuditsAndDelete(path);
            }
        });
    }

    // 创建新的"requeue.tmp"文件用于存储为成功发送的Audit
    // No need to be thread safe, queuePath have to be a unique temporary file
    public void requeueMissingAuditsAndDelete(Path toSendFilePath) {
        File toSendFile = toSendFilePath.toFile();
        if (!toSendFile.exists() || toSendFile.length() == 0) {
            return;
        }
        File tmpRequeueFile = new File(toSendFile.getAbsolutePath() + REQUEUE_FILE_SUFFIX);
        try {
            auditAlreadySent = new HashSet<>();
            FileHelper.applyOnFiles(toSendFile, toSendFilePath.getFileName().toString() + ACKNOWLEDGE_STRING,
                    path -> auditAlreadySent.add(path.getFileName().toString()));

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(toSendFile), "UTF-8"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpRequeueFile, false), "UTF-8"))) {
                requeueMissingLine(toSendFilePath, reader, writer);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            finalizeRequeue(tmpRequeueFile);
            deleteQueueTempFiles(toSendFile, toSendFilePath);
        }
    }

    // TODO. 过滤掉已经成功发送到Audit Event事件
    private void requeueMissingLine(Path toSendFilePath, BufferedReader reader, BufferedWriter writer) throws IOException {
        String line;
        long auditCounter = 0L;
        while ((line = reader.readLine()) != null) {
            try {
                ++auditCounter;
                String pattern = String.format(toSendFilePath.getFileName() + ACKNOWLEDGE_FILE_PATTERN, auditCounter);
                if (auditAlreadySent.contains(pattern)) {
                    continue;
                }

                writer.write(line);
                writer.newLine();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // TODO. 合并JSON文件中Audits到Requeue文件中, 再重命名成JSON文件
    private void finalizeRequeue(File tmpRequeueFile) {
        if (null == tmpRequeueFile || !tmpRequeueFile.exists() || tmpRequeueFile.length() == 0) {
            return;
        }

        // TODO. 必须加锁处理, 避免多个线程写入操作数据丢失
        synchronized (queueJsonFile) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(queueJsonFile), "UTF-8"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpRequeueFile, true), "UTF-8"))) {
                IOUtils.copyLarge(reader, writer);
            } catch (Exception e) {
                e.printStackTrace();
            }

            try {
                queueJsonFile.delete();
                Files.move(tmpRequeueFile.toPath(), queueJsonFile.toPath(), StandardCopyOption.ATOMIC_MOVE);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void deleteQueueTempFiles(File toSendFile, Path toSendFilePath) {
        FileHelper.applyOnFiles(toSendFile, toSendFilePath.getFileName().toString(),
            path -> {
                try {
                    Files.deleteIfExists(path);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        toSendFile.delete();
    }
}