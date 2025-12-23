package com.audit.client;

import com.audit.client.model.AuditEntry;
import com.audit.client.util.FileHelper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

// 关于Audit Queue队列管理器
public class AuditQueueManager {

    private File queueJsonFile;
    private String queueFileName = "./audit/eventqueue.json";
    public final static int MAX_QUEUE_SIZE = 50000;

    private final static String TO_SEND_FILE_SUFFIX = ".to_send.txt";
    private final static String ACKNOWLEDGE_STRING = ".result.OK.";
    private final static String ACKNOWLEDGE_FILE_PATTERN = ".result.OK.%06d.tmp";
    private final static String REQUEUE_FILE_SUFFIX = ".requeue.tmp";

    private final ObjectMapper jsonMapper;

    public AuditQueueManager() throws IOException {
        this.queueJsonFile = new File(queueFileName);
        queueJsonFile.getParentFile().mkdirs();
        queueJsonFile.createNewFile();
        jsonMapper = new ObjectMapper();
    }

    // 判断是否有Audit Event在队列中(一行代表一个Event)
    public boolean isQueueEmpty() {
        synchronized (queueJsonFile) {
            return (queueJsonFile.length() == 0);
        }
    }

    // 写入到Queue JSON文件中的Event必须加锁
    public void queueNewAudit(AuditEntry entry) {
        synchronized (queueJsonFile) {
            try (BufferedWriter writer = new BufferedWriter(
                    new OutputStreamWriter(new FileOutputStream(queueJsonFile, true), "UTF-8"))) {
                writer.write(jsonMapper.writeValueAsString(entry));
                writer.newLine();
            } catch (Exception e) {
                System.out.println("Unable to queue audit event list " + e.getMessage());
            }
        }
    }

    // 将Queue JSON文件数据拷贝到待处理文件中, 重新创建新JSON文件
    // Return an unique audit queue copy file and reset the main audit queue
    public Path getPendingAudits() {
        synchronized (queueJsonFile) {
            if (!isQueueEmpty() && queueJsonFile.exists()) {
                try {
                    Path pendingQueue = Paths.get(queueJsonFile.getAbsolutePath() + "_"
                            + UUID.randomUUID() + TO_SEND_FILE_SUFFIX).toAbsolutePath().normalize();
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

    // 完成单一Audit事件的发送，生成对应的结果文件
    public void completeSendAudit(File queueFile, int auditCount, File tmpFile) throws IOException {
        final String finalFileName = String.format(queueFile.getAbsolutePath() + ACKNOWLEDGE_FILE_PATTERN, auditCount);
        final Path finalStatusPath = Paths.get(finalFileName).toAbsolutePath().normalize();
        Files.move(tmpFile.toPath(), finalStatusPath, StandardCopyOption.ATOMIC_MOVE);
    }

    // 将待处理(未成功处理)的Event重新恢复
    // Restore missing audit events from temporary files in case of crash
    public void restoreMissingAudits() {
        FileHelper.applyOnFiles("Restore initial audit queue", queueJsonFile, queueJsonFile.getName(), path -> {
            if (path.toString().endsWith(TO_SEND_FILE_SUFFIX)) {
               requeueMissingAuditsAndDelete(path);
            }
        });
    }

    // No need to be thread safe, queuePath have to be a unique temporary file
    public void requeueMissingAuditsAndDelete(Path toSendFilePath) {
        File toSendFile = toSendFilePath.toFile();
        if (!toSendFile.exists() || toSendFile.length() == 0) {
            return;
        }

        File tmpRequeueFile = new File(toSendFile.getAbsolutePath() + REQUEUE_FILE_SUFFIX);
        try {
            Set<String> auditAlreadySent = new HashSet<>();
            FileHelper.applyOnFiles("retrieve audit already sent", toSendFile,
                    toSendFilePath.getFileName() + ACKNOWLEDGE_STRING,
                    path -> auditAlreadySent.add(path.getFileName().toString()));

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(toSendFile), "UTF-8"));
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tmpRequeueFile, false), "UTF-8"))) {

                String line;
                long auditCounter = 0;
                while ((line = reader.readLine()) != null) {
                    try {
                        // TODO. 过滤掉已经成功发送到Audit Event事件
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
            } catch (Exception e) {
                e.printStackTrace();
            }
        } finally {
            finalizeRequeue(tmpRequeueFile);
            deleteQueueTempFiles(toSendFile, toSendFilePath);
        }
    }

    // TODO. 将JSON文件中新增Event拷贝到Requeue文件中, 再重命名成JSON文件
    private void finalizeRequeue(File tmpRequeueFile) {
        if (null == tmpRequeueFile || !tmpRequeueFile.exists() || tmpRequeueFile.length() == 0) {
            return;
        }

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
        FileHelper.applyOnFiles("delete temporary audit file", toSendFile,
                toSendFilePath.getFileName().toString(),
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
